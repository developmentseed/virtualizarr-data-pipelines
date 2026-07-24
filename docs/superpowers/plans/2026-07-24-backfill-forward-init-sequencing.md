# Backfill / Forward Init Ownership + Sequencing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When backfill is enabled, make the backfill Step Functions `Init` the sole store bootstrap and deploy the forward SQS consumer disabled until an explicit setting re-enables it.

**Architecture:** Two CDK-level changes driven by settings. (1) A new `FORWARD_QUEUE_ENABLED` setting (`bool | None`, resolved by a pydantic `model_validator` to "disabled when backfill is on, enabled otherwise") feeds the SQS event source's `enabled` flag. (2) The deploy-time forward-init block (`initialize_icechunk_lambda` + its trigger/custom-resource) is gated behind `if not settings.BACKFILL_ENABLED:`, so `initialize_backfill_store` owns bootstrap when backfill is on. No processor/protocol/write-path changes.

**Tech Stack:** AWS CDK (Python, `aws-cdk-lib`), `pydantic` / `pydantic-settings`, pytest with `aws_cdk.assertions.Template` (synth-time; no Docker or AWS).

**Spec:** `docs/superpowers/specs/2026-07-24-backfill-forward-init-sequencing-design.md`

---

## File Structure

- `cdk/settings.py` — add `FORWARD_QUEUE_ENABLED` field + `model_validator` resolver. (Owns configuration.)
- `cdk/stack.py` — pass `enabled=settings.FORWARD_QUEUE_ENABLED` to the `SqsEventSource`; wrap the forward-init block in `if not settings.BACKFILL_ENABLED:`. (Owns infrastructure wiring.)
- `tests/cdk/test_settings.py` — unit test for the resolver. (Owns settings tests.)
- `tests/cdk/test_stack_gating.py` — synth-time assertions for the event-source `enabled` matrix and init gating. (Owns stack synth tests.)
- `.env.sample` — document the new setting.

**Out of plan (owner-managed):** `README.md` runbook subsection — the repo owner is editing the README directly; do not modify it as part of this plan.

---

## Task 1: Add `FORWARD_QUEUE_ENABLED` setting + resolver

**Files:**
- Modify: `cdk/settings.py`
- Test: `tests/cdk/test_settings.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/cdk/test_settings.py`:

```python
def test_forward_queue_enabled_defaults_on_when_backfill_off() -> None:
    settings = StackSettings(STAGE="dev", ACCOUNT_ID="111111111111")
    assert settings.FORWARD_QUEUE_ENABLED is True


def test_forward_queue_enabled_defaults_off_when_backfill_on() -> None:
    settings = StackSettings(
        STAGE="dev", ACCOUNT_ID="111111111111", BACKFILL_ENABLED=True
    )
    assert settings.FORWARD_QUEUE_ENABLED is False


def test_forward_queue_enabled_explicit_value_is_honored() -> None:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        BACKFILL_ENABLED=True,
        FORWARD_QUEUE_ENABLED=True,
    )
    assert settings.FORWARD_QUEUE_ENABLED is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cdk/test_settings.py -v`
Expected: FAIL — the three new tests error/fail because `FORWARD_QUEUE_ENABLED` does not exist yet (pydantic will not have the attribute / it is `None`).

- [ ] **Step 3: Write minimal implementation**

In `cdk/settings.py`, change the pydantic import line:

```python
from pydantic import model_validator
from pydantic_settings import BaseSettings
```

Then add the field and resolver at the end of the `StackSettings` class body, immediately after `BACKFILL_MAX_CONCURRENCY: int = 50`:

```python
    # Forward SQS consumer. `None` resolves in the validator below:
    #   backfill enabled  -> default disabled (bootstrap via backfill, enable later)
    #   backfill disabled -> default enabled  (normal forward-only deployment)
    FORWARD_QUEUE_ENABLED: bool | None = None

    @model_validator(mode="after")
    def _resolve_forward_queue_enabled(self) -> "StackSettings":
        if self.FORWARD_QUEUE_ENABLED is None:
            self.FORWARD_QUEUE_ENABLED = not self.BACKFILL_ENABLED
        return self
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cdk/test_settings.py -v`
Expected: PASS (all tests, including the pre-existing `test_backfill_settings_defaults`).

- [ ] **Step 5: Commit**

```bash
git add cdk/settings.py tests/cdk/test_settings.py
git commit -m "feat: add FORWARD_QUEUE_ENABLED setting resolving from BACKFILL_ENABLED"
```

---

## Task 2: Drive the SQS event source `enabled` flag from the setting

**Files:**
- Modify: `cdk/stack.py` (the `SqsEventSource` in `add_event_source`)
- Test: `tests/cdk/test_stack_gating.py`

- [ ] **Step 1: Write the failing test**

In `tests/cdk/test_stack_gating.py`, add a flexible template helper and make the existing `_synth` delegate to it, then add the event-source tests. Replace the existing `_synth` function with:

```python
def _template(*, backfill: bool, forward: bool | None = None) -> Template:
    kwargs = dict(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        DATA_BUCKET_NAME="data-test",
        BACKFILL_ENABLED=backfill,
    )
    if forward is not None:
        kwargs["FORWARD_QUEUE_ENABLED"] = forward
    settings = StackSettings(**kwargs)
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


def _synth(enabled: bool) -> Template:
    return _template(backfill=enabled)
```

Then append the new tests:

```python
def test_forward_queue_enabled_when_backfill_off() -> None:
    _template(backfill=False).has_resource_properties(
        "AWS::Lambda::EventSourceMapping", {"Enabled": True}
    )


def test_forward_queue_disabled_when_backfill_on() -> None:
    _template(backfill=True).has_resource_properties(
        "AWS::Lambda::EventSourceMapping", {"Enabled": False}
    )


def test_forward_queue_explicit_enable_with_backfill_on() -> None:
    _template(backfill=True, forward=True).has_resource_properties(
        "AWS::Lambda::EventSourceMapping", {"Enabled": True}
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cdk/test_stack_gating.py -v`
Expected: FAIL — `test_forward_queue_disabled_when_backfill_on` fails because the event source currently emits no `Enabled: false` (the property is unset today, defaulting to enabled).

- [ ] **Step 3: Write minimal implementation**

In `cdk/stack.py`, add the `enabled` argument to the `SqsEventSource`. Replace:

```python
        self.process_messages_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.queue,
                batch_size=settings.SQS_BATCH_SIZE,
                report_batch_item_failures=True,
                max_concurrency=settings.MAX_CONCURRENCY,
            )
        )
```

with:

```python
        self.process_messages_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.queue,
                batch_size=settings.SQS_BATCH_SIZE,
                report_batch_item_failures=True,
                max_concurrency=settings.MAX_CONCURRENCY,
                enabled=settings.FORWARD_QUEUE_ENABLED,
            )
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cdk/test_stack_gating.py -v`
Expected: PASS (new tests plus the pre-existing `test_backfill_disabled_creates_no_state_machine` / `test_backfill_enabled_creates_state_machine`).

- [ ] **Step 5: Commit**

```bash
git add cdk/stack.py tests/cdk/test_stack_gating.py
git commit -m "feat: gate forward SQS consumer on FORWARD_QUEUE_ENABLED"
```

---

## Task 3: Gate the deploy-time forward init on `not BACKFILL_ENABLED`

**Files:**
- Modify: `cdk/stack.py` (the `initialize_icechunk_lambda` block through the `else` custom-resource branch)
- Test: `tests/cdk/test_stack_gating.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/cdk/test_stack_gating.py`:

```python
def _resource_ids(template: Template) -> str:
    return " ".join(template.to_json()["Resources"].keys()).lower()


def test_backfill_disabled_creates_initialize_lambda() -> None:
    assert "initializeicechunk" in _resource_ids(_template(backfill=False))


def test_backfill_enabled_skips_initialize_lambda() -> None:
    assert "initializeicechunk" not in _resource_ids(_template(backfill=True))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cdk/test_stack_gating.py::test_backfill_enabled_skips_initialize_lambda -v`
Expected: FAIL — the initialize lambda is still synthesized when backfill is enabled, so the substring `initializeicechunk` is present.

- [ ] **Step 3: Write minimal implementation**

In `cdk/stack.py`, wrap the entire forward-init block in `if not settings.BACKFILL_ENABLED:` and indent it one level. Replace this block:

```python
        self.initialize_icechunk_lambda = _lambda.DockerImageFunction(
            self,
            f"{settings.STACK_NAME}-initialize-icechunk-lambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="lambda",
                file="initialize/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.minutes(5),
            memory_size=2048,
        )

        self.icechunk_bucket.grant_read_write(self.initialize_icechunk_lambda)

        if settings.ICECHUNK_BUCKET:
            # Trigger it once on first deploy
            self.trigger = cr.AwsCustomResource(
                self,
                "TriggerOnce",
                on_create=cr.AwsSdkCall(
                    service="Lambda",
                    action="invoke",
                    parameters={
                        "FunctionName": self.initialize_icechunk_lambda.function_name,
                        "InvocationType": "Event",
                    },
                    physical_resource_id=cr.PhysicalResourceId.of("trigger-once-id"),
                ),
                policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                    resources=[self.initialize_icechunk_lambda.function_arn]
                ),
            )

            self.trigger.node.add_dependency(self.initialize_icechunk_lambda)
        else:
            self.custom_resource_provider = cr.Provider(
                self,
                "S3BucketCustomResourceProvider",
                on_event_handler=self.initialize_icechunk_lambda,
            )

            self.bucket_custom_resource = CustomResource(
                self,
                "S3BucketCustomResource",
                service_token=self.custom_resource_provider.service_token,
                properties={
                    "BucketName": self.icechunk_bucket.bucket_name,
                },
            )

            self.bucket_custom_resource.node.add_dependency(self.icechunk_bucket)
```

with (note the leading `if` and the extra 4-space indent on every line):

```python
        # When backfill is enabled, initialize_backfill_store (the Step Functions
        # Init step) is the sole store bootstrap. Skipping the deploy-time seed
        # avoids a create_array("foo", ...) collision on `main`.
        if not settings.BACKFILL_ENABLED:
            self.initialize_icechunk_lambda = _lambda.DockerImageFunction(
                self,
                f"{settings.STACK_NAME}-initialize-icechunk-lambda",
                code=_lambda.DockerImageCode.from_image_asset(
                    directory="lambda",
                    file="initialize/Dockerfile",
                    platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
                ),
                architecture=_lambda.Architecture.X86_64,
                timeout=Duration.minutes(5),
                memory_size=2048,
            )

            self.icechunk_bucket.grant_read_write(self.initialize_icechunk_lambda)

            if settings.ICECHUNK_BUCKET:
                # Trigger it once on first deploy
                self.trigger = cr.AwsCustomResource(
                    self,
                    "TriggerOnce",
                    on_create=cr.AwsSdkCall(
                        service="Lambda",
                        action="invoke",
                        parameters={
                            "FunctionName": self.initialize_icechunk_lambda.function_name,
                            "InvocationType": "Event",
                        },
                        physical_resource_id=cr.PhysicalResourceId.of(
                            "trigger-once-id"
                        ),
                    ),
                    policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                        resources=[self.initialize_icechunk_lambda.function_arn]
                    ),
                )

                self.trigger.node.add_dependency(self.initialize_icechunk_lambda)
            else:
                self.custom_resource_provider = cr.Provider(
                    self,
                    "S3BucketCustomResourceProvider",
                    on_event_handler=self.initialize_icechunk_lambda,
                )

                self.bucket_custom_resource = CustomResource(
                    self,
                    "S3BucketCustomResource",
                    service_token=self.custom_resource_provider.service_token,
                    properties={
                        "BucketName": self.icechunk_bucket.bucket_name,
                    },
                )

                self.bucket_custom_resource.node.add_dependency(self.icechunk_bucket)
```

Note: `self.initialize_icechunk_lambda`, `self.trigger`, etc. are referenced only inside this block, so gating it out leaves no dangling references. If a grep (`grep -n "initialize_icechunk_lambda\|self.trigger" cdk/stack.py`) shows any reference outside this block, stop and re-scope — but there are none today.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cdk/test_stack_gating.py -v`
Expected: PASS — including `test_backfill_disabled_creates_initialize_lambda` (substring present) and `test_backfill_enabled_skips_initialize_lambda` (absent).

- [ ] **Step 5: Commit**

```bash
git add cdk/stack.py tests/cdk/test_stack_gating.py
git commit -m "feat: skip deploy-time forward init when backfill is enabled"
```

---

## Task 4: Document the setting in `.env.sample`

**Files:**
- Modify: `.env.sample`

- [ ] **Step 1: Add the setting after a clean anchor line**

The last line of `.env.sample` is a pre-existing truncated fragment (`ACCOUNT_`) — leave it as-is (out of scope). Insert the new setting after the `GARBAGE_COLLECTION_FREQUENCY=2` line so it lands on its own clean line. The file should contain this line among its settings:

```
# Forward SQS consumer. Leave unset to default: enabled normally, but disabled
# automatically when BACKFILL_ENABLED=true (so the backfill bootstraps main first).
# After the backfill promotes, set FORWARD_QUEUE_ENABLED=true and redeploy.
FORWARD_QUEUE_ENABLED=false
```

- [ ] **Step 2: Verify synth still resolves settings from the sample**

Run: `uv run --env-file .env.sample cdk synth 2>&1 | head -5`
Expected: synth begins without a pydantic validation error about `FORWARD_QUEUE_ENABLED` (it is optional). A synth that proceeds past settings loading is success for this step. (If synth fails for an unrelated pre-existing `.env.sample` reason, that is out of scope for this task — confirm the failure is not about `FORWARD_QUEUE_ENABLED`.)

- [ ] **Step 3: Commit**

```bash
git add .env.sample
git commit -m "docs: document FORWARD_QUEUE_ENABLED in .env.sample"
```

---

## Final verification

- [ ] Run the full test suite:

Run: `uv run pytest -q`
Expected: all tests pass (previously 34 + the new settings/gating tests).

- [ ] Run lint on the changed files:

Run: `uv run ruff check cdk/settings.py cdk/stack.py tests/cdk/test_settings.py tests/cdk/test_stack_gating.py`
Expected: `All checks passed!`

---

## Self-Review

**Spec coverage:**
- Init single-owner (skip deploy seed when backfill enabled) → Task 3.
- Forward consumer deployed disabled + re-enabled via setting → Tasks 1 & 2.
- Safe-by-default (`None` resolves to disabled when backfill on) → Task 1 resolver + Task 1 tests.
- No change to `initialize_repo`/protocol/write paths → confirmed; only `cdk/` + tests + `.env.sample` touched.
- Testing matrix (event-source `enabled` across three cases; init present/absent; settings resolution) → Tasks 1–3 tests.
- `.env.sample` documented → Task 4. README left to owner → noted "Out of plan".

**Placeholder scan:** none — every code step shows full code; commands have expected output.

**Type consistency:** `FORWARD_QUEUE_ENABLED` is `bool | None` on the field and always a `bool` after the validator; `SqsEventSource(enabled=...)` accepts `Optional[bool]`. Setting/attribute names (`FORWARD_QUEUE_ENABLED`, `BACKFILL_ENABLED`, `_resolve_forward_queue_enabled`) are consistent across settings, stack, and tests. Test helper `_template(*, backfill, forward=None)` signature matches all call sites.
