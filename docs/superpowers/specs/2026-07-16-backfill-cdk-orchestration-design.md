# Sub-project C: Backfill CDK Step Functions orchestration

**Date:** 2026-07-16
**Issue:** [#12 — Refactor backfill processing to use partitioned fork and merge approach](https://github.com/developmentseed/virtualizarr-data-pipelines/issues/12)
**Status:** Design approved, pending spec review
**Overview:** `docs/superpowers/backfill-pipeline-overview.md`
**Depends on:** sub-project A (Processor backfill interface) and sub-project B (Lambda handlers +
partitioner), both complete on this branch.
**Completes:** the backfill pipeline (A → B → C).

## Background

Sub-project A built the backfill data model (Processor methods + fork/merge helpers), and
sub-project B built the six Lambda handlers (`partition`, `init`, `fork`, `worker`, `reduce`,
`promote`) with their JSON I/O contracts, tested with moto + local filesystem. Sub-project C is
the AWS orchestration: it packages the handlers as Lambda functions and wires them into a Step
Functions state machine (an outer serial `Map` over partitions containing an inner
`DistributedMap` of workers), deployed via CDK and gated by a setting. This is the layer the
handler I/O contracts in the overview were designed for.

## Scope

In scope:

- One `lambda/backfill/Dockerfile` packaging the `backfill_handlers` package.
- A `BackfillPipeline` CDK construct (`cdk/stack_constructs/backfill_pipeline.py`) that creates
  the six Lambda functions and the Step Functions state machine, instantiated in
  `VirtualizarrSqsStack` and gated by a new setting.
- New `StackSettings` fields for enablement and tuning.
- A small amendment to sub-project B's `partition` handler: also emit `manifest_key`.
- CDK synth-time template tests.

Out of scope:

- Any real AWS deployment or integration test (synth-time assertions only).
- Changes to the operational append pipeline (`process_messages`, `initialize`, GC).

## Design decisions (from brainstorming)

1. **One image, six functions via CMD override.** A single `lambda/backfill/Dockerfile` builds
   the package once; CDK creates six `DockerImageFunction`s from that one image asset, each
   overriding `cmd=["backfill_handlers.<name>.handler"]`.
2. **`run_prefix` derives from the Step Functions execution name** —
   `States.Format('s3://{bucket}/backfill/{}/', $$.Execution.Name)` — so each run is uniquely
   isolated and traceable; the caller supplies only `inventory_uri`.
3. **Tuning is deploy-time via `StackSettings`** (`partition_size`, `MaxItemsPerBatch`,
   `MaxConcurrency`) — matches the one-deployment-per-dataset template model and avoids dynamic
   Distributed Map `*Path` wiring.
4. **A new `BackfillPipeline` construct in the existing stack, gated by a setting**
   (`BACKFILL_ENABLED`), mirroring how `GARBAGE_COLLECTION_FREQUENCY` gates the Batch resources.
5. **Dynamic inner ItemReader key.** Verified: passing `sfn.JsonPath.string_at("$.manifest_key")`
   as the `S3JsonItemReader` `key` renders to `"Key.$":"$.manifest_key"` in the ASL — no escape
   hatch needed (CDK 2.232.2).

## Components

### 1. Packaging — `lambda/backfill/Dockerfile`

Multi-stage, mirroring `lambda/process_messages/Dockerfile` (build context `lambda/`): copy
`virtualizarr-processor` and `backfill` (the `backfill_handlers` package), `uv pip install` into
`/var/task`. The `CMD` is a placeholder; each Lambda function overrides it via CDK `cmd`.

### 2. Sub-project B amendment — `partition` handler emits `manifest_key`

The inner Distributed Map's `S3JsonItemReader` needs `bucket` + object `key`. The `partition`
handler currently returns `{partition_id, manifest_uri}`; add `manifest_key` (the S3 object key,
e.g. `backfill/<run>/partitions/0.json`) so the ItemReader uses `bucket=<icechunk bucket>` +
`key=JsonPath("$.manifest_key")`. One field + one test assertion; cleaner than S3-URI string
surgery in ASL. `manifest_uri` stays (used elsewhere / for humans).

### 3. `BackfillPipeline` construct — `cdk/stack_constructs/backfill_pipeline.py`

A `Construct` (keyword-only args, matching `BatchInfra`/`BatchJob`) that takes the icechunk
bucket, data bucket name, and settings, and builds:

**Six `DockerImageFunction`s** from one `DockerImageCode.from_image_asset("lambda",
file="backfill/Dockerfile")`, each with `cmd=["backfill_handlers.<name>.handler"]`, env
(`ICECHUNK_BUCKET`, `ICECHUNK_PREFIX`, `ICECHUNK_REGION`), timeout/memory per role. IAM:

- `init`, `fork`, `reduce`, `promote`, `partition`: read/write on the icechunk bucket.
- `partition`: additionally read the inventory object (icechunk bucket, or a configured location).
- `worker`: read/write on the icechunk bucket (fork blobs) **and** `s3:GetObject` + `s3:ListBucket`
  on the data bucket — `f"arn:aws:s3:::{DATA_BUCKET_NAME}/*"` and
  `f"arn:aws:s3:::{DATA_BUCKET_NAME}"` — because `process_backfill_file` runs a VirtualiZarr
  parser over the source files (reading + listing) to compute the virtual references.

**The state machine** (Standard):

```
Partition (LambdaInvoke)
  payload: { inventory_uri: $.inventory_uri,
             run_prefix: States.Format('s3://<bucket>/backfill/{}/', $$.Execution.Name),
             partition_size: <setting> }
  result:  { partitions: [ {partition_id, manifest_uri, manifest_key}, … ] }
Init (LambdaInvoke)            build full-shape store + commit (clean base)
Map over $.partitions   (MaxConcurrency = 1, SERIAL)
  ├─ Fork (LambdaInvoke)       → { partition_id, manifest_uri, manifest_key,
  │                                fork_in_uri, forks_out_prefix }
  ├─ DistributedMap
  │    ItemReader   = S3JsonItemReader(bucket=<icechunk>, key=JsonPath "$.manifest_key")
  │    ItemBatcher  = ItemBatcher(max_items_per_batch = <setting>)
  │    MaxConcurrency = <setting>
  │    ItemSelector merges each file-key batch with the constant
  │                 fork_in_uri / forks_out_prefix → worker event
  │    ItemProcessor = Worker (LambdaInvoke)
  │    result_path  = JsonPath.DISCARD        (reducer lists S3; outputs not aggregated)
  │    tolerated_failure_count = 0            (a worker failure fails the partition)
  └─ Reduce (LambdaInvoke)     list child forks → merge → one commit
Promote (LambdaInvoke)         reset_branch main → backfill tip
```

### 4. `StackSettings` additions — `cdk/settings.py`

- `BACKFILL_ENABLED: bool = False` — gates the whole `BackfillPipeline` construct.
- `BACKFILL_PARTITION_SIZE: int` (e.g. default 500) — passed to the `partition` handler.
- `BACKFILL_MAX_ITEMS_PER_BATCH: int` (e.g. default 10) — inner `ItemBatcher`.
- `BACKFILL_MAX_CONCURRENCY: int` (e.g. default 50) — inner `DistributedMap` concurrency.

### 5. Stack wiring — `cdk/stack.py`

In `VirtualizarrSqsStack`, when `settings.BACKFILL_ENABLED`, instantiate `BackfillPipeline`,
passing the icechunk bucket, `DATA_BUCKET_NAME`, and settings — same conditional style as the GC
block.

## Data flow

Execution input `{ "inventory_uri": "s3://…/inventory.json" }` → `partition` writes per-partition
manifests under `run_prefix` and returns the partition list → `init` builds the full-shape store
→ serial `Map`: per partition `fork` writes one shared fork blob, the inner `DistributedMap`
reads that partition's manifest (dynamic key), batches its file keys, fans them to parallel
`worker` invocations (each writing a child fork blob), then `reduce` merges the child forks into
one commit → after all partitions, `promote` fast-forwards `main`.

## Error handling

- **Serial outer Map** (MaxConcurrency 1): partitions run one at a time and commit independently,
  so a failure never corrupts completed partitions (incremental recovery).
- **Inner Distributed Map `tolerated_failure_count = 0`:** a worker failure fails the partition
  rather than silently dropping data; `reduce` never runs on a partial/empty fork set (this
  resolves the empty-child-forks edge case flagged in sub-project B's review). The `worker`
  handler already raises on a failed file.
- **LambdaInvoke retries:** transient-error retry with backoff (`Lambda.ServiceException`,
  `Lambda.TooManyRequestsException`, throttling) on each task.
- **Operational note (documented, not coded):** `initialize_backfill_store` requires the
  `backfill` branch not to pre-exist, so re-running a whole backfill against an existing store
  needs the `backfill` branch cleaned up first. A within-run partition retry is safe (workers
  re-write child blobs; duplicate disjoint chunks are last-writer-wins → correct values).

## Testing

CDK synth-time assertions via `aws_cdk.assertions.Template` (no deploy):

- **Enabled** (`BACKFILL_ENABLED=True`): six Lambda functions with the expected
  `cmd=["backfill_handlers.<name>.handler"]` overrides; one `AWS::StepFunctions::StateMachine`
  whose definition contains the serial outer `Map` (MaxConcurrency 1), the inner `DistributedMap`
  with the dynamic `"Key.$":"$.manifest_key"` ItemReader, `ItemBatcher` (MaxItemsPerBatch), and
  MaxConcurrency; the worker's data-bucket `s3:GetObject`/`s3:ListBucket` grant and the icechunk
  bucket grants.
- **Disabled** (default): none of the backfill resources are synthesized (gating works).
- **B amendment unit test:** `partition.handler` output includes `manifest_key` equal to the S3
  object key of the written manifest (moto).

## Deliverable

1. `lambda/backfill/Dockerfile`.
2. `partition` handler emits `manifest_key` (+ test).
3. `BackfillPipeline` construct with the six functions, IAM, and the state machine.
4. `StackSettings` additions + conditional wiring in `VirtualizarrSqsStack`.
5. CDK synth-time template tests (enabled + disabled) and the B-amendment unit test.
