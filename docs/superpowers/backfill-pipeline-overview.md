# Backfill Pipeline — High-Level Workflow Overview

**Issue:** [#12 — Refactor backfill processing to use partitioned fork and merge approach](https://github.com/developmentseed/virtualizarr-data-pipelines/issues/12)
**Status:** living overview — links the sub-project specs and records the agreed end-to-end workflow.

This document is the map. Each sub-project has its own detailed spec and plan under
`docs/superpowers/specs/` and `docs/superpowers/plans/`.

## Why

The operational pipeline appends on `main`, committing once per SQS batch. For large backfills,
many parallel workers contend for the `main` branch tip, causing a "thundering herd" of rebase
waits. The backfill pipeline instead initializes the store at **full shape** on a `backfill`
branch, fans out workers that each write a **disjoint** subset of chunk references into
independent Icechunk **forks**, merges the forks into **one commit per partition**, and finally
fast-forwards `main`. This maximizes the writes-per-commit ratio and removes tip contention.

## Proven mechanics (icechunk 1.1.14 / virtualizarr 2.2.1)

Verified by sub-project A, sub-project B's end-to-end test, and the low-level
mechanics regression tests in `tests/backfill_mechanics/` (which graduated the two
unique proofs — real cross-process `spawn` fork/merge, and merge's last-writer-wins
overlap behaviour — out of the original disposable spike):

- Store is created at full shape; workers write disjoint chunks via `set_virtual_ref`
  (VirtualiZarr `to_icechunk` only appends, so it can't target a region).
- `Session.fork()` produces a picklable `ForkSession`; a writable `Session` is NOT picklable and
  a `ForkSession` cannot commit.
- A **fresh** `writable_session` (in a later process) can `merge(*forks)` and commit, as long as
  the fork's base is a **committed** branch-tip snapshot (init commits first → clean base).
- One shared fork can be distributed to N workers, each calling `fork()` for its own child; a
  fresh session merges all children and commits once. This is the shape Step Functions
  Distributed Map needs (per-worker batching happens inside the map).
- `merge` does NOT detect chunk overlap (last-writer-wins) — **disjointness is the operator's
  responsibility** (the inventory must map files to distinct regions).
- Backfill requires **durable** storage (S3, or local filesystem in tests); a pickled
  `ForkSession` cannot resolve its base snapshot from `in_memory_storage`.

## Decomposition

| Sub-project | Scope | Spec |
|-------------|-------|------|
| **A** | Backfill Processor interface + synthetic reference impl (pure Python) | `specs/2026-06-25-backfill-processor-interface-design.md` |
| **B** | Lambda handlers + partitioner (the Lambda layer; tested with moto + local FS) | `specs/2026-07-15-backfill-lambda-handlers-design.md` |
| **C** | CDK Step Functions orchestration (outer state machine + inner Distributed Map, real S3) | `specs/2026-07-16-backfill-cdk-orchestration-design.md` |

## End-to-end workflow (outer Step Function)

```
[Partitioner]   read S3 inventory → split into partition manifests in S3
      │         → returns the partition list
      ▼
[Init]          open_backfill_repo() → initialize_backfill_store(): create `backfill`
      │         branch + full-shape store, COMMIT → clean base snapshot
      ▼
Map over partitions   (MaxConcurrency = 1, SERIAL — incremental failure recovery)
┌─────────────────────────────────────────────────────────────────────────┐
│  [Fork]     open_backfill_repo() → create_fork() → write ONE shared fork  │
│             artifact to s3://…/forks/{partition}/in/fork.pkl              │
│      ▼                                                                    │
│  Inner Distributed Map   (ItemReader = partition manifest,               │
│                           ItemBatcher = MaxItemsPerBatch,                 │
│                           MaxConcurrency = parallel workers)             │
│     [Worker] load shared fork → fork() child → process_backfill_file      │
│       per key → write child fork to s3://…/forks/{partition}/out/{uuid}   │
│       (worker never opens the repo)                                       │
│      ▼                                                                    │
│  [Reduce]   open_backfill_repo() → list forks/{partition}/out/ →          │
│             merge_and_commit(...) → ONE commit for the partition          │
└─────────────────────────────────────────────────────────────────────────┘
      │  (next partition; its fork bases off the new committed tip)
      ▼
[Promote]   open_backfill_repo() → promote(): reset_branch("main", backfill tip)
```

Per-run isolation: every S3 location derives from a per-run prefix, e.g.
`s3://<bucket>/backfill/<run-id>/`.

## Handler I/O contracts (the B ↔ C interface)

JSON event-in / result-out for each Lambda handler. Sub-project C's state machine produces and
consumes these.

**`partition`**
- in: `{ "inventory_uri", "run_prefix", "partition_size" }`
- out: `{ "partitions": [ { "partition_id", "manifest_uri" }, … ] }`

**`init`**
- in: `{ }` (repo config from env)
- out: `{ "base_snapshot" }`

**`fork`**
- in: `{ "partition_id", "manifest_uri", "run_prefix" }`
- out: `{ "partition_id", "manifest_uri", "fork_in_uri", "forks_out_prefix" }`

**`worker`**
- in: `{ "fork_in_uri", "forks_out_prefix", "file_keys": [ … batch … ] }`
- out: `{ "child_fork_uri" }`

**`reduce`**
- in: `{ "partition_id", "forks_out_prefix" }`
- out: `{ "partition_id", "tip" }`

**`promote`**
- in: `{ }`
- out: `{ "promoted": true }`

Notes:
- The per-run prefix threads through partition → fork → worker/reduce.
- `worker` returns `child_fork_uri`, but `reduce` independently **lists** `forks_out_prefix` —
  the reducer does not depend on collecting worker outputs (belt-and-suspenders).
- `worker` never opens the repo; its payload is only the shared fork + the file-key batch.

## Repo opening

`open_backfill_repo()` is a `VirtualizarrProcessor` Protocol method (added in sub-project B). The
reference implementation is env-configurable: `icechunk.s3_storage(...)` when `ICECHUNK_BUCKET`
is set (Lambda), otherwise `local_filesystem_storage(<path from env>)` (tests). It uses
`open_or_create`, which yields a `main` branch for `initialize_backfill_store` to branch off.
