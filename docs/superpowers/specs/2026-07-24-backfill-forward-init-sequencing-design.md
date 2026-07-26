# Backfill / Forward Init Ownership + Sequencing — Design

**Status:** approved design, ready for implementation plan.
**Related:** [backfill pipeline overview](../backfill-pipeline-overview.md),
issue [#12](https://github.com/developmentseed/virtualizarr-data-pipelines/issues/12).

## Problem

The store can be bootstrapped by two different paths:

- **Forward** — a deploy-time custom resource (`TriggerOnce`) invokes
  `initialize_icechunk_lambda`, which calls `Processor().initialize_repo()` and seeds a
  small, growable store on `main` (append-along-`time`).
- **Backfill** — the Step Functions `Init` step calls `initialize_backfill_store()`, which
  branches off `main` and creates the array(s) at their **full shape**, then `Promote`
  fast-forwards `main` at the end of the run.

With `BACKFILL_ENABLED=true` both fire, and they conflict: the deploy seed creates `foo`
on `main`, then `initialize_backfill_store`'s `create_array("foo", …)` collides with the
already-seeded array. Beyond init, running the forward consumer against `main` *before*
the backfill has promoted is also unsafe — `main` has no store yet (or a store that
`Promote`'s `reset_branch` will overwrite).

This design makes initialization single-owner when backfill is enabled, and sequences the
two pipelines so the forward consumer does not run until the backfill has promoted. This is
"Option A" (sequence them); it deliberately does **not** attempt simultaneous
backfill + forward writes (that is "Option B", out of scope here).

## Goals

- When backfill is enabled, `initialize_backfill_store` is the **sole** store bootstrap;
  the deploy-time forward seed does not run.
- The forward SQS consumer is deployed **disabled** while backfill is the active bootstrap,
  and is re-enabled explicitly (declaratively, via a setting + redeploy) once the backfill
  has promoted to `main`.
- Safe-by-default: enabling backfill without saying anything about the forward queue must
  not leave a live consumer appending to a not-yet-initialized `main`.
- No change to `initialize_repo`, the `VirtualizarrProcessor` protocol, the
  `promote`/`reset_branch` mechanic, or the write paths.

## Non-goals

- Simultaneous backfill and forward writes against a live `main` (Option B: merge-into-main
  instead of `reset_branch`, horizon enforcement). Explicitly deferred.
- Any change to how disjointness is guaranteed within a backfill run.

## Design

### 1. Init ownership — gate the deploy-time forward seed

Wrap the entire forward-init block in `cdk/stack.py` — the `initialize_icechunk_lambda`
`DockerImageFunction`, its `icechunk_bucket.grant_read_write(...)`, and **both** the
`TriggerOnce` `AwsCustomResource` branch and the `else` provider/`CustomResource` branch —
in `if not settings.BACKFILL_ENABLED:`.

Result:

- `BACKFILL_ENABLED=false` → unchanged: deploy seeds `main` via `initialize_repo`.
- `BACKFILL_ENABLED=true` → no deploy seed; `main` is bootstrapped only when the backfill
  state machine runs (`Init` → fill → `Promote`).

`initialize_repo` itself is unchanged and still exists — pure-forward deployments and the
garbage-collection path continue to use it. Only the *timing/existence of the deploy-time
trigger* changes.

### 2. Forward-queue toggle + setting resolution

Add a setting that drives the SQS event source's `enabled` flag, with a `None` default that
resolves based on `BACKFILL_ENABLED`:

```python
# cdk/settings.py  (StackSettings)
FORWARD_QUEUE_ENABLED: bool | None = None

@model_validator(mode="after")
def _resolve_forward_queue(self) -> "StackSettings":
    if self.FORWARD_QUEUE_ENABLED is None:
        self.FORWARD_QUEUE_ENABLED = not self.BACKFILL_ENABLED
    return self
```

```python
# cdk/stack.py
self.process_messages_lambda.add_event_source(
    lambda_event_sources.SqsEventSource(
        self.queue,
        batch_size=settings.SQS_BATCH_SIZE,
        report_batch_item_failures=True,
        enabled=settings.FORWARD_QUEUE_ENABLED,
    )
)
```

The `None`-default resolver removes the main footgun: enabling backfill without mentioning
the forward queue defaults the consumer **disabled**; pure-forward deployments default
**enabled**. An explicit value always wins.

`model_validator` is imported from `pydantic` (pydantic v2, already a dependency via
`pydantic_settings`).

### 3. Behavior matrix

| BACKFILL_ENABLED | FORWARD_QUEUE_ENABLED | Deploy-time seed | Forward consumer |
|---|---|---|---|
| false | (unset → true) | runs (`initialize_repo`) | enabled |
| true | (unset → false) | skipped | disabled |
| true | true (post-backfill) | skipped | enabled |
| true | false | skipped | disabled |

### 4. Bootstrap runbook

1. Deploy with `BACKFILL_ENABLED=true` (forward queue resolves to disabled). SQS buffers
   any notifications that arrive.
2. Run the backfill state machine → `Init` (full-shape store) → fill → `Promote` (`main`
   now complete).
3. Set `FORWARD_QUEUE_ENABLED=true` and `cdk deploy` again → the consumer starts, drains the
   buffered queue, and appends new files after the backfilled range.

## Components touched

- `cdk/settings.py` — add `FORWARD_QUEUE_ENABLED` + `model_validator` resolver.
- `cdk/stack.py` — gate the forward-init block on `not BACKFILL_ENABLED`; pass
  `enabled=settings.FORWARD_QUEUE_ENABLED` to the `SqsEventSource`.
- `tests/cdk/` — synth-time assertions (below) + a settings unit test.
- `.env.sample` — add `FORWARD_QUEUE_ENABLED` with an explanatory comment.
- `README.md` — a runbook subsection (owner is actively editing the README; coordinate on
  timing — the spec/plan is the source of truth).

## Testing

Synth-time only (`Template.from_stack` / `app.synth()`, no Docker or AWS):

- `BACKFILL_ENABLED=false` (default): the initialize custom resource is present, and the
  `AWS::Lambda::EventSourceMapping` has `Enabled: true`.
- `BACKFILL_ENABLED=true`, forward unset: no initialize custom resource / no initialize
  lambda synthesized, and the `EventSourceMapping` has `Enabled: false`.
- `BACKFILL_ENABLED=true, FORWARD_QUEUE_ENABLED=true`: `EventSourceMapping` `Enabled: true`.
- `cdk/settings` unit test: the `None`-resolution rule yields the matrix above across the
  `(BACKFILL_ENABLED, FORWARD_QUEUE_ENABLED)` combinations, and an explicit value is honored.

## Error handling / edge cases

- **Backfill on, forward queue accidentally left enabled:** guarded by the safe default; an
  operator who explicitly sets `FORWARD_QUEUE_ENABLED=true` too early owns that choice
  (documented in the runbook).
- **Re-deploys are idempotent:** the toggle is declarative, so there is no config drift
  between deploys (the reason a setting is preferred over a manual `update-event-source-mapping`).
- **Pure-forward deployments:** no behavior change — the deploy seed still runs and the
  consumer is enabled by default.
