# Sub-project B: Backfill Lambda handlers + partitioner

**Date:** 2026-07-15
**Issue:** [#12 — Refactor backfill processing to use partitioned fork and merge approach](https://github.com/developmentseed/virtualizarr-data-pipelines/issues/12)
**Status:** Design approved, pending spec review
**Overview:** `docs/superpowers/backfill-pipeline-overview.md`
**Depends on:** sub-project A (`specs/2026-06-25-backfill-processor-interface-design.md`) —
`initialize_backfill_store`, `region_for`, `process_backfill_file`, and the `backfill.py`
helpers (`create_fork`, `merge_and_commit`, `promote`).
**Followed by:** sub-project C (CDK Step Functions orchestration + Dockerfiles + real S3).

## Background

Sub-project A built the backfill data model as pure-Python Processor methods and generic
fork/merge helpers, proven locally. Sub-project B builds the **Lambda layer**: the handler
functions that sub-project C's Step Functions state machine will invoke, plus the partitioner
that seeds the run. B is testable end-to-end locally with `moto` (mocked S3) and
`local_filesystem_storage` (repo) — no AWS and no CDK.

The end-to-end workflow and the proven icechunk mechanics are documented in the overview
(`docs/superpowers/backfill-pipeline-overview.md`); this spec details the handler layer only.

## Scope

In scope:

- A new Protocol method `open_backfill_repo()` + its env-configurable reference implementation.
- A new package `lambda/backfill/` with six handler modules (`partition`, `init`, `fork`,
  `worker`, `reduce`, `promote`) and two support modules (`inventory`, `fork_store`) plus
  `config`.
- A `pyproject.toml` for the package so it is importable/testable.
- Local tests: unit tests per module + a full end-to-end handler chain test (moto + local FS).

Out of scope (sub-project C):

- Dockerfiles for the handlers, CDK constructs, the Step Functions state machine and Distributed
  Map wiring, IAM, and any real-S3 / real-Lambda integration.

Not changed: the operational append path (`process_messages`, existing `initialize`) and the
existing `Processor` append methods.

## Design decisions (from brainstorming)

1. **Durable repo opening is a Protocol method.** `open_backfill_repo()` is added to
   `VirtualizarrProcessor`; the reference `Processor` implements it env-configurably (S3 in
   Lambda, local filesystem in tests). Chosen over a handler-layer opener so storage choice lives
   with the user's Processor (the template pattern).
2. **Partitioner input is an inventory file in S3** — a single object listing the file keys,
   prepared ahead of time. (Not a runtime prefix listing, not an inline event list.)
3. **The partitioner just splits — no disjointness verification.** Disjointness is the operator's
   responsibility (the inventory must map files to distinct regions). YAGNI.
4. **Testing: `moto` for S3 + `local_filesystem_storage` for the repo.** Hermetic, no AWS creds.

## Components

### 1. Protocol method — `virtualizarr_processor/typing.py` (+ reference impl in `processor.py`)

- `open_backfill_repo(self) -> Repository` — open (or create) the durable backfill repository.
  Reference implementation:
  - If `ICECHUNK_BUCKET` is set: `icechunk.s3_storage(bucket=..., prefix=..., region=...,
    from_env=True)` (Lambda IAM credentials).
  - Else: `icechunk.local_filesystem_storage(<ICECHUNK_LOCAL_PATH from env>)` (tests).
  - Applies the same `RepositoryConfig` + `VirtualChunkContainer` + `authorize_virtual_chunk_access`
    the reference `initialize_repo` uses, and calls `Repository.open_or_create` (which yields a
    `main` branch for `initialize_backfill_store` to branch off).

  Adding this method keeps the `@runtime_checkable` conformance test green because the reference
  `Processor` implements it.

### 2. Handler package — `lambda/backfill/`

Package `backfill_handlers/` (small, single-responsibility modules), each handler decorated with
powertools `@logger.inject_lambda_context()` / `@tracer.capture_lambda_handler`, matching the
existing `initialize` / `process_messages` convention.

Support modules:

- **`config.py`** — parse environment into a small settings object: repo storage selection
  (bucket/prefix/region vs. local path), and any handler defaults. Pure functions over
  `os.environ`.
- **`inventory.py`** — file-list I/O over S3 (boto3): `read_inventory(uri) -> list[str]`
  (read the inventory object, one key per line or JSON array), `write_manifest(uri, keys)` and
  `read_manifest(uri) -> list[str]` (partition manifests as JSON arrays).
- **`fork_store.py`** — fork-blob I/O over S3 (boto3): `save_fork(uri, data: bytes)`,
  `load_fork(uri) -> bytes`, `list_forks(prefix) -> list[str]`.

Handler modules (each exposes `handler(event, context)`):

- **`partition.py`** — read the inventory via `inventory.read_inventory`, chunk into groups of
  `partition_size`, write each group to `<run_prefix>partitions/{i}.json` via
  `inventory.write_manifest`, return the partition list.
- **`init.py`** — `repo = processor.open_backfill_repo()`;
  `snapshot = processor.initialize_backfill_store(repo)`; return `{ "base_snapshot": snapshot }`.
- **`fork.py`** — `repo = open_backfill_repo()`; `blob = backfill.create_fork(repo)`;
  `fork_store.save_fork(<run_prefix>forks/{partition_id}/in/fork.pkl, blob)`; return the
  fork/out URIs.
- **`worker.py`** — `blob = fork_store.load_fork(fork_in_uri)`;
  `child = pickle.loads(blob).fork()`; for each `key` in `file_keys`,
  `processor.process_backfill_file(key, child)`;
  `fork_store.save_fork(<forks_out_prefix>{uuid}.pkl, pickle.dumps(child))`. Does NOT open the
  repo. (The `uuid` is generated per invocation; in tests it is injected/monkeypatched for
  determinism.)
- **`reduce.py`** — `repo = open_backfill_repo()`;
  `uris = fork_store.list_forks(forks_out_prefix)`;
  `children = [fork_store.load_fork(u) for u in uris]`;
  `tip = backfill.merge_and_commit(repo, children, message=f"Backfill partition {partition_id}")`;
  return `{ "partition_id", "tip" }`.
- **`promote.py`** — `repo = open_backfill_repo()`; `backfill.promote(repo)`;
  return `{ "promoted": True }`.

### 3. Packaging — `lambda/backfill/pyproject.toml`

Declares the package with dependencies `aws-lambda-powertools`, `aws-xray-sdk`, `boto3`,
`icechunk`, and `virtualizarr-processor` (uv workspace source `../virtualizarr-processor`), and a
dev dependency `moto` for tests. (Dockerfile is deferred to sub-project C.)

## Handler I/O contracts

The full JSON event-in / result-out contracts are the canonical B↔C interface and are recorded
in `docs/superpowers/backfill-pipeline-overview.md` under "Handler I/O contracts". Summary:

| Handler | in | out |
|---------|----|----|
| `partition` | `inventory_uri, run_prefix, partition_size` | `partitions: [{partition_id, manifest_uri}]` |
| `init` | `{}` (env) | `base_snapshot` |
| `fork` | `partition_id, manifest_uri, run_prefix` | `partition_id, manifest_uri, fork_in_uri, forks_out_prefix` |
| `worker` | `fork_in_uri, forks_out_prefix, file_keys[]` | `child_fork_uri` |
| `reduce` | `partition_id, forks_out_prefix` | `partition_id, tip` |
| `promote` | `{}` (env) | `promoted: true` |

Per-run isolation: all S3 locations derive from `run_prefix` (e.g.
`s3://<bucket>/backfill/<run-id>/`). `worker` returns `child_fork_uri` but `reduce` independently
**lists** `forks_out_prefix`, so the reducer does not depend on collecting worker outputs.

## Data flow

`partition` seeds `partitions/*.json` from the inventory. `init` builds the full-shape store and
commits (clean base). For each partition (serial, driven by C's outer Map): `fork` writes one
shared fork blob; the inner Distributed Map batches the partition's file keys and fans them to
`worker` invocations, each writing a child fork blob; `reduce` lists and merges the child forks
into one commit. After all partitions, `promote` fast-forwards `main`.

## Error handling

- `process_backfill_file` already returns `bool` (from A); `worker` logs and treats a `False` as
  a failed file. Whether a failed file fails the batch is decided by C's Distributed Map retry
  config; the worker surfaces failures via a raised exception when it cannot write its child fork
  at all.
- `reduce` lets `merge_and_commit` errors propagate (the state machine owns retry/rebase policy).
- Because partitions are serial and each commits independently, a failed partition can be retried
  alone without redoing completed partitions (the issue's incremental-recovery goal).
- Disjointness is assumed (operator responsibility); `merge` will not detect overlap.

## Testing

Local pytest, hermetic:

- **`moto`** provides a mocked S3 (`mock_aws`) with a test bucket; `inventory`, `fork_store`, and
  every handler exercise real boto3 calls against it.
- **`open_backfill_repo`** points at a `tmp_path` via env → `local_filesystem_storage` (proven to
  work with forks); the repo persists across handler calls within a test.
- Unit tests: `config` env parsing; `inventory` round-trips; `fork_store` round-trips + listing;
  `open_backfill_repo` returns a usable repo with a `main` branch; `test_follows_protocol` stays
  green.
- Per-handler tests: invoke each `handler(event, context)` with a synthetic event, assert the
  result contract and side effects (objects written, branch/commit created). `worker` uses an
  injected/monkeypatched uuid for a deterministic child-fork key.
- **End-to-end handler test (capstone):** chain `partition → init → fork → worker×N → reduce →
  promote` in one test against moto S3 + a single local-FS repo, then open `main` and assert every
  slice resolves to its expected synthetic value — A's round-trip proven through the handler layer
  with S3 I/O semantics.

## Deliverable

1. `open_backfill_repo` Protocol method + env-configurable reference implementation.
2. `lambda/backfill/` package: `config`, `inventory`, `fork_store`, and the six handler modules,
   plus `pyproject.toml`.
3. Passing local test suite: unit tests, per-handler tests, and the end-to-end chain test.
