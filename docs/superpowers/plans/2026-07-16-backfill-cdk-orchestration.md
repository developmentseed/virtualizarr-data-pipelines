# Backfill CDK Orchestration (sub-project C) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the backfill Lambda handlers as a Step Functions pipeline via CDK — one Docker image → six functions (CMD override), an outer serial `Map` over partitions containing an inner `DistributedMap` of workers (dynamic per-partition ItemReader), gated by a setting — with CDK synth-time tests.

**Architecture:** A `BackfillPipeline` construct builds six `DockerImageFunction`s from a single image asset and a Standard state machine: Partition → Init → serial Map[Fork → DistributedMap(worker batches) → Reduce] → Promote. `run_prefix` derives from the execution name; tuning comes from `StackSettings`. The construct is instantiated in `VirtualizarrSqsStack` only when `BACKFILL_ENABLED`.

**Tech Stack:** AWS CDK (Python) 2.232.2, `aws_cdk.aws_stepfunctions` (DistributedMap, S3JsonItemReader, ItemBatcher), `aws_cdk.assertions.Template`, `pytest`, `uv`. Every CDK snippet below was prototyped and its rendered ASL / template inspected against these versions.

**Spec:** `docs/superpowers/specs/2026-07-16-backfill-cdk-orchestration-design.md`
**Overview:** `docs/superpowers/backfill-pipeline-overview.md`

---

## Critical implementation notes (verified while planning)

1. **Template tests need no Docker.** `DockerImageFunction(DockerImageCode.from_image_asset(...))`
   + `Template.from_stack(...)` synthesizes with the Docker daemon **down** — the image build is
   deferred to deploy/asset-staging, not synth. So the CDK tests run hermetically. (The referenced
   Dockerfile must still *exist* for asset staging to hash the context — Task 3 creates it first.)
2. **`tolerated_failure_count=0` is the Distributed Map default and CDK omits it from the ASL.**
   Do NOT pass it and do NOT assert `ToleratedFailureCount` in the template — a Distributed Map
   with no `ToleratedFailure*` already fails on any child failure, which is the intended behavior.
   Add a code comment saying so.
3. **`payload_response_only=True`** on every `LambdaInvoke` so the state output is the handler's
   returned dict (not the Lambda invoke envelope).
4. **The batched worker event is reshaped in the state machine, so B's `worker` handler is
   unchanged.** `ItemBatcher(batch_input={...})` yields `{"BatchInput": {...}, "Items": [...]}`
   per iteration; the WorkerTask payload maps `file_keys ← $.Items`,
   `fork_in_uri ← $.BatchInput.fork_in_uri`, `forks_out_prefix ← $.BatchInput.forks_out_prefix`.
5. **The isolated pre-commit mypy env has no `aws_cdk`** → all `aws_cdk`/`constructs` imports
   resolve to `Any`. Functions must still be fully typed (`disallow_untyped_defs`); a `-> None`
   `__init__` and typed params suffice. No casts are needed in the construct (it returns nothing).
6. CDK test files import `settings`/`stack`/`stack_constructs.*`, which live under `cdk/` (not on
   the default pytest path). A `tests/cdk/conftest.py` inserts `cdk/` on `sys.path` (Task 3).

## File Structure

- **Modify `lambda/backfill/backfill_handlers/partition.py`** — also emit `manifest_key`.
- **Modify `tests/backfill_handlers/test_partition.py`** — assert `manifest_key`.
- **Modify `cdk/settings.py`** — add `BACKFILL_ENABLED` + three tuning fields.
- **Create `lambda/backfill/Dockerfile`** — package `backfill_handlers` (mirrors `process_messages`).
- **Create `cdk/stack_constructs/backfill_pipeline.py`** — the `BackfillPipeline` construct.
- **Modify `cdk/stack_constructs/__init__.py`** — export `BackfillPipeline`.
- **Modify `cdk/stack.py`** — conditionally instantiate `BackfillPipeline`.
- **Create `tests/cdk/conftest.py`** — put `cdk/` on `sys.path`.
- **Create `tests/cdk/test_settings.py`, `tests/cdk/test_backfill_pipeline.py`, `tests/cdk/test_stack_gating.py`** — CDK tests.

No changes to the operational append pipeline.

---

### Task 1: `partition` handler also emits `manifest_key`

**Files:**
- Modify: `lambda/backfill/backfill_handlers/partition.py`
- Test: `tests/backfill_handlers/test_partition.py`

- [ ] **Step 1: Update the test to assert `manifest_key`**

In `tests/backfill_handlers/test_partition.py`, add assertions to
`test_partition_splits_inventory_into_manifests` after the existing ones:

```python
    # manifest_key is the S3 object key of the manifest (for the Distributed Map ItemReader).
    assert parts[0]["manifest_key"] == "run/partitions/0.json"
    assert parts[2]["manifest_key"] == "run/partitions/2.json"
```

(The `run_prefix` in that test is `f"s3://{s3_bucket}/run/"`, so the key is `run/partitions/N.json`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_partition.py -v`
Expected: FAIL with `KeyError: 'manifest_key'`.

- [ ] **Step 3: Emit `manifest_key` in the handler**

In `lambda/backfill/backfill_handlers/partition.py`, add the import
`from backfill_handlers.config import parse_s3_uri` at the top (next to
`from backfill_handlers import inventory`). Then, the loop currently builds `manifest_uri` and
appends `{"partition_id", "manifest_uri"}`; add the object key derived with `parse_s3_uri`.
Replace the loop with:

```python
    # run_prefix is s3://<bucket>/<prefix>/; parse_s3_uri returns (bucket, "<prefix>/").
    _, run_key_prefix = parse_s3_uri(run_prefix)
    partitions: list[dict[str, str]] = []
    for i in range(0, len(keys), size):
        partition_id = str(i // size)
        manifest_key = f"{run_key_prefix}partitions/{partition_id}.json"
        manifest_uri = f"{run_prefix}partitions/{partition_id}.json"
        inventory.write_manifest(manifest_uri, keys[i : i + size])
        partitions.append(
            {
                "partition_id": partition_id,
                "manifest_uri": manifest_uri,
                "manifest_key": manifest_key,
            }
        )
```

For `run_prefix = "s3://bucket/run/"`, `parse_s3_uri` returns `("bucket", "run/")`, so
`manifest_key = "run/partitions/0.json"` — the S3 object key the Distributed Map ItemReader needs
(paired with `bucket=<icechunk bucket>`).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_partition.py -v`
Expected: PASS.

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add lambda/backfill/backfill_handlers/partition.py tests/backfill_handlers/test_partition.py
git commit -m "feat: partition handler emits manifest_key for the Distributed Map ItemReader"
```

---

### Task 2: `StackSettings` backfill fields

**Files:**
- Modify: `cdk/settings.py`
- Create: `tests/cdk/conftest.py`, `tests/cdk/test_settings.py`

- [ ] **Step 1: Write the conftest + failing test**

Create `tests/cdk/conftest.py`:

```python
import os
import sys

# cdk/ holds top-level modules (settings, stack, stack_constructs) that are not on
# the default pytest path; add it so the CDK tests can import them.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "cdk"))
```

Create `tests/cdk/test_settings.py`:

```python
from settings import StackSettings


def test_backfill_settings_defaults() -> None:
    settings = StackSettings(STAGE="dev", ACCOUNT_ID="111111111111")
    assert settings.BACKFILL_ENABLED is False
    assert settings.BACKFILL_PARTITION_SIZE == 500
    assert settings.BACKFILL_MAX_ITEMS_PER_BATCH == 10
    assert settings.BACKFILL_MAX_CONCURRENCY == 50
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cdk/test_settings.py -v`
Expected: FAIL with `AttributeError: 'StackSettings' object has no attribute 'BACKFILL_ENABLED'`.

- [ ] **Step 3: Add the fields**

In `cdk/settings.py`, inside `class StackSettings`, add (near the other config fields):

```python
    # Backfill (partitioned fork/merge) pipeline
    BACKFILL_ENABLED: bool = False
    BACKFILL_PARTITION_SIZE: int = 500
    BACKFILL_MAX_ITEMS_PER_BATCH: int = 10
    BACKFILL_MAX_CONCURRENCY: int = 50
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cdk/test_settings.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cdk/settings.py tests/cdk/conftest.py tests/cdk/test_settings.py
git commit -m "feat: backfill StackSettings fields (enable + tuning)"
```

---

### Task 3: Dockerfile + `BackfillPipeline` functions & IAM

**Files:**
- Create: `lambda/backfill/Dockerfile`, `cdk/stack_constructs/backfill_pipeline.py`
- Modify: `cdk/stack_constructs/__init__.py`
- Test: `tests/cdk/test_backfill_pipeline.py`

- [ ] **Step 1: Write the failing test**

Create `tests/cdk/test_backfill_pipeline.py`:

```python
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
from aws_cdk.assertions import Match, Template
from stack_constructs.backfill_pipeline import BackfillPipeline


def _template() -> Template:
    app = cdk.App()
    stack = cdk.Stack(
        app, "TestStack", env=cdk.Environment(account="111111111111", region="us-east-1")
    )
    bucket = s3.Bucket(stack, "IceBucket")
    BackfillPipeline(
        stack,
        "Backfill",
        icechunk_bucket=bucket,
        data_bucket_name="my-data-bucket",
        partition_size=500,
        max_items_per_batch=10,
        max_concurrency=50,
    )
    return Template.from_stack(stack)


def test_six_functions_with_cmd_overrides() -> None:
    template = _template()
    template.resource_count_is("AWS::Lambda::Function", 6)
    for action in ["partition", "init", "fork", "worker", "reduce", "promote"]:
        template.has_resource_properties(
            "AWS::Lambda::Function",
            Match.object_like(
                {"ImageConfig": {"Command": [f"backfill_handlers.{action}.handler"]}}
            ),
        )


def test_worker_has_data_bucket_read_and_list() -> None:
    template = _template()
    template.has_resource_properties(
        "AWS::IAM::Policy",
        Match.object_like(
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": ["s3:GetObject", "s3:ListBucket"],
                                    "Resource": [
                                        "arn:aws:s3:::my-data-bucket/*",
                                        "arn:aws:s3:::my-data-bucket",
                                    ],
                                }
                            )
                        ]
                    )
                }
            }
        ),
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cdk/test_backfill_pipeline.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'stack_constructs.backfill_pipeline'`.

- [ ] **Step 3: Create the Dockerfile**

Create `lambda/backfill/Dockerfile` (mirrors `lambda/process_messages/Dockerfile`; build context
is `lambda/`; the CMD is a placeholder overridden per-function by CDK):

```dockerfile
# Build stage
FROM public.ecr.aws/lambda/python:3.12 AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /build/lambda
COPY virtualizarr-processor ./virtualizarr-processor
COPY backfill ./backfill

WORKDIR /build/lambda/backfill
RUN uv pip install --python /var/lang/bin/python3.12 --target /var/task --no-cache .

# Runtime stage
FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /var/task /var/task

CMD ["backfill_handlers.partition.handler"]
```

- [ ] **Step 4: Create the construct (functions + IAM)**

Create `cdk/stack_constructs/backfill_pipeline.py`:

```python
from typing import Any

from aws_cdk import Aws, Duration
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lmb
from aws_cdk import aws_s3 as s3
from constructs import Construct

_ACTIONS = ["partition", "init", "fork", "worker", "reduce", "promote"]


class BackfillPipeline(Construct):
    """Backfill Step Functions pipeline: six Lambda handlers built from one image,
    wired into an outer serial Map over partitions with an inner Distributed Map of
    workers."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        icechunk_bucket: s3.IBucket,
        data_bucket_name: str,
        partition_size: int,
        max_items_per_batch: int,
        max_concurrency: int,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.functions: dict[str, lmb.DockerImageFunction] = {}
        for action in _ACTIONS:
            fn = lmb.DockerImageFunction(
                self,
                f"{action}-fn",
                code=lmb.DockerImageCode.from_image_asset(
                    "lambda",
                    file="backfill/Dockerfile",
                    platform=ecr_assets.Platform.LINUX_AMD64,
                    cmd=[f"backfill_handlers.{action}.handler"],
                ),
                architecture=lmb.Architecture.X86_64,
                timeout=Duration.minutes(15),
                memory_size=2048,
                environment={
                    "ICECHUNK_BUCKET": icechunk_bucket.bucket_name,
                    "ICECHUNK_REGION": Aws.REGION,
                },
            )
            icechunk_bucket.grant_read_write(fn)
            self.functions[action] = fn

        # worker parses source files; partition reads the inventory object.
        data_policy = iam.PolicyStatement(
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::{data_bucket_name}/*",
                f"arn:aws:s3:::{data_bucket_name}",
            ],
        )
        self.functions["worker"].add_to_role_policy(data_policy)
        self.functions["partition"].add_to_role_policy(data_policy)
```

(The `partition_size` / `max_items_per_batch` / `max_concurrency` params are unused in this task —
Task 4 uses them when it builds the state machine at the end of `__init__`. Ruff's selected rules
(E, F, I) do not flag unused function arguments, so leave them in the signature.)

- [ ] **Step 5: Export the construct**

In `cdk/stack_constructs/__init__.py`, add `BackfillPipeline` to the imports/exports alongside
`BatchInfra`/`BatchJob` (match the existing style, e.g.
`from stack_constructs.backfill_pipeline import BackfillPipeline` and add to `__all__` if present).

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/cdk/test_backfill_pipeline.py -v`
Expected: PASS (both tests).

- [ ] **Step 7: Run the whole suite**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add lambda/backfill/Dockerfile cdk/stack_constructs/backfill_pipeline.py cdk/stack_constructs/__init__.py tests/cdk/test_backfill_pipeline.py
git commit -m "feat: BackfillPipeline construct - six functions from one image + IAM"
```

---

### Task 4: State machine in the construct

**Files:**
- Modify: `cdk/stack_constructs/backfill_pipeline.py`
- Test: `tests/cdk/test_backfill_pipeline.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/cdk/test_backfill_pipeline.py`:

```python
import json


def _state_machine_asl() -> str:
    app = cdk.App()
    stack = cdk.Stack(
        app, "TestStack", env=cdk.Environment(account="111111111111", region="us-east-1")
    )
    bucket = s3.Bucket(stack, "IceBucket")
    BackfillPipeline(
        stack,
        "Backfill",
        icechunk_bucket=bucket,
        data_bucket_name="my-data-bucket",
        partition_size=500,
        max_items_per_batch=10,
        max_concurrency=50,
    )
    tmpl = app.synth().get_stack_by_name("TestStack").template
    for res in tmpl["Resources"].values():
        if res["Type"] == "AWS::StepFunctions::StateMachine":
            parts = res["Properties"]["DefinitionString"]["Fn::Join"][1]
            return "".join(p if isinstance(p, str) else "<REF>" for p in parts)
    raise AssertionError("no state machine synthesized")


def test_state_machine_shape() -> None:
    template = _template()
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)

    asl = _state_machine_asl()
    # inner Distributed Map with a dynamic per-partition ItemReader key
    assert '"Mode":"DISTRIBUTED"' in asl
    assert '"Key.$":"$.forkResult.manifest_key"' in asl
    assert '"MaxItemsPerBatch":10' in asl
    # outer Map is serial
    assert '"MaxConcurrency":1' in asl
    # worker event reshape (Items -> file_keys, BatchInput carries the constants)
    assert '"file_keys.$":"$.Items"' in asl
    assert "$.BatchInput.fork_in_uri" in asl
    # run_prefix derives from the execution name
    assert "Execution.Name" in asl
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cdk/test_backfill_pipeline.py::test_state_machine_shape -v`
Expected: FAIL with `AssertionError: no state machine synthesized`.

- [ ] **Step 3: Build the state machine**

Add these imports at the top of `cdk/stack_constructs/backfill_pipeline.py`:

```python
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
```

At the very end of `BackfillPipeline.__init__` (after the two `add_to_role_policy` lines), add the
state-machine build:

```python
        self.state_machine = self._build_state_machine(
            icechunk_bucket, partition_size, max_items_per_batch, max_concurrency
        )
```

Then add this method to the class:

```python
    def _build_state_machine(
        self,
        icechunk_bucket: s3.IBucket,
        partition_size: int,
        max_items_per_batch: int,
        max_concurrency: int,
    ) -> sfn.StateMachine:
        partition = tasks.LambdaInvoke(
            self,
            "PartitionTask",
            lambda_function=self.functions["partition"],
            payload=sfn.TaskInput.from_object(
                {
                    "inventory_uri": sfn.JsonPath.string_at("$.inventory_uri"),
                    "run_prefix": sfn.JsonPath.format(
                        "s3://{}/backfill/{}/",
                        icechunk_bucket.bucket_name,
                        sfn.JsonPath.string_at("$$.Execution.Name"),
                    ),
                    "partition_size": partition_size,
                }
            ),
            payload_response_only=True,
            result_path="$.partitionResult",
        )

        init = tasks.LambdaInvoke(
            self,
            "InitTask",
            lambda_function=self.functions["init"],
            payload=sfn.TaskInput.from_object({}),
            payload_response_only=True,
            result_path="$.initResult",
        )

        fork = tasks.LambdaInvoke(
            self,
            "ForkTask",
            lambda_function=self.functions["fork"],
            payload_response_only=True,
            result_path="$.forkResult",
        )

        worker = tasks.LambdaInvoke(
            self,
            "WorkerTask",
            lambda_function=self.functions["worker"],
            payload=sfn.TaskInput.from_object(
                {
                    "file_keys": sfn.JsonPath.string_at("$.Items"),
                    "fork_in_uri": sfn.JsonPath.string_at("$.BatchInput.fork_in_uri"),
                    "forks_out_prefix": sfn.JsonPath.string_at(
                        "$.BatchInput.forks_out_prefix"
                    ),
                }
            ),
            payload_response_only=True,
        )

        inner_map = sfn.DistributedMap(
            self,
            "InnerMap",
            item_reader=sfn.S3JsonItemReader(
                bucket=icechunk_bucket,
                key=sfn.JsonPath.string_at("$.forkResult.manifest_key"),
            ),
            item_batcher=sfn.ItemBatcher(
                max_items_per_batch=max_items_per_batch,
                batch_input={
                    "fork_in_uri": sfn.JsonPath.string_at("$.forkResult.fork_in_uri"),
                    "forks_out_prefix": sfn.JsonPath.string_at(
                        "$.forkResult.forks_out_prefix"
                    ),
                },
            ),
            max_concurrency=max_concurrency,
            # No tolerated_failure_*: the Distributed Map default fails on any worker
            # failure, so a partition never reduces on an incomplete fork set.
            result_path=sfn.JsonPath.DISCARD,
        )
        inner_map.item_processor(worker)

        reduce = tasks.LambdaInvoke(
            self,
            "ReduceTask",
            lambda_function=self.functions["reduce"],
            payload_response_only=True,
            result_path="$.reduceResult",
        )

        outer_map = sfn.Map(
            self,
            "OuterMap",
            items_path="$.partitionResult.partitions",
            max_concurrency=1,
            result_path=sfn.JsonPath.DISCARD,
        )
        outer_map.item_processor(fork.next(inner_map).next(reduce))

        promote = tasks.LambdaInvoke(
            self,
            "PromoteTask",
            lambda_function=self.functions["promote"],
            payload=sfn.TaskInput.from_object({}),
            payload_response_only=True,
        )

        definition = partition.next(init).next(outer_map).next(promote)
        return sfn.StateMachine(
            self,
            "StateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/cdk/test_backfill_pipeline.py -v`
Expected: PASS (all three tests — the two from Task 3 still pass; the state machine test passes).

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add cdk/stack_constructs/backfill_pipeline.py tests/cdk/test_backfill_pipeline.py
git commit -m "feat: BackfillPipeline state machine (serial Map + inner Distributed Map)"
```

---

### Task 5: Stack wiring + gating

**Files:**
- Modify: `cdk/stack.py`
- Test: `tests/cdk/test_stack_gating.py`

- [ ] **Step 1: Write the failing test**

Create `tests/cdk/test_stack_gating.py`:

```python
import aws_cdk as cdk
from aws_cdk.assertions import Template
from settings import StackSettings
from stack import VirtualizarrSqsStack


def _synth(enabled: bool) -> Template:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        DATA_BUCKET_NAME="data-test",
        BACKFILL_ENABLED=enabled,
    )
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


def test_backfill_disabled_creates_no_state_machine() -> None:
    _synth(False).resource_count_is("AWS::StepFunctions::StateMachine", 0)


def test_backfill_enabled_creates_state_machine() -> None:
    _synth(True).resource_count_is("AWS::StepFunctions::StateMachine", 1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cdk/test_stack_gating.py -v`
Expected: `test_backfill_enabled_creates_state_machine` FAILS (0 state machines — construct not
wired yet); the disabled test passes.

- [ ] **Step 3: Wire the construct into the stack**

In `cdk/stack.py`, add the import near the other `stack_constructs` import:

```python
from stack_constructs import BackfillPipeline, BatchInfra, BatchJob
```

Then, at the end of `VirtualizarrSqsStack.__init__` (after the GC block), add:

```python
        if settings.BACKFILL_ENABLED:
            self.backfill_pipeline = BackfillPipeline(
                self,
                "BackfillPipeline",
                icechunk_bucket=self.icechunk_bucket,
                data_bucket_name=settings.DATA_BUCKET_NAME,
                partition_size=settings.BACKFILL_PARTITION_SIZE,
                max_items_per_batch=settings.BACKFILL_MAX_ITEMS_PER_BATCH,
                max_concurrency=settings.BACKFILL_MAX_CONCURRENCY,
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/cdk/test_stack_gating.py -v`
Expected: both PASS.

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add cdk/stack.py tests/cdk/test_stack_gating.py
git commit -m "feat: gate BackfillPipeline behind BACKFILL_ENABLED in the stack"
```

---

## Notes for the implementer

- All CDK snippets were prototyped and their rendered ASL / template inspected on aws-cdk-lib
  2.232.2. If a synth/assertion diverges, that is a finding — report it.
- Template tests do NOT require Docker (verified with the daemon down). Do not add Docker to CI
  for these.
- Do NOT pass `tolerated_failure_count` — the Distributed Map default already fails on any worker
  failure, and CDK omits a `0` from the ASL (the state-machine test would fail if you asserted it).
- `mypy` in pre-commit has no `aws_cdk`; keep functions fully typed (`-> None`, typed params). No
  casts needed here.
- This completes the backfill pipeline (A → B → C). No further sub-projects.
```
