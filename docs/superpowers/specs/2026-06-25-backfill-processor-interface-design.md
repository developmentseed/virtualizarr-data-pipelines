# Sub-project A: Backfill Processor interface + reference implementation

**Date:** 2026-06-25
**Issue:** [#12 — Refactor backfill processing to use partitioned fork and merge approach](https://github.com/developmentseed/virtualizarr-data-pipelines/issues/12)
**Status:** Design approved, pending spec review
**Depends on:** the proven fork/merge mechanics in
`docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md`
**Part of:** the larger backfill pipeline, decomposed as A (this doc) → B (Lambda handlers +
partitioner) → C (CDK Step Functions orchestration).

## Background

The operational pipeline ingests data by appending on `main`: SQS → `process_messages`
Lambda → one Icechunk commit per batch. For large backfills, many parallel workers contending
for the `main` branch tip cause a "thundering herd" of rebase waits.

The spike (see the spike spec) proved an alternative on icechunk 1.1.14 / virtualizarr 2.2.1:
initialize the store at **full shape** on a `backfill` branch, fan out workers that each write a
disjoint subset of chunk references into independent Icechunk forks, then merge all forks into a
single commit. Crucially, the spike also proved (during this sub-project's brainstorming) the
**decoupled** variant required by Step Functions, where each stage is a separate process/Lambda:

- A writable `Session` cannot be pickled and a `ForkSession` cannot commit, so the
  `fork → merge → commit` chain cannot ship a live session between processes.
- BUT a **fresh** `writable_session` (opened in a later process) can `merge(*forks)` and commit,
  as long as the session that created the forks was **clean** (zero uncommitted changes) at
  `fork()` time — i.e. the fork's base is just the committed branch-tip snapshot. Verified.
- A single shared fork can be distributed to N workers, each calling `fork()` to make its own
  child, writing disjoint refs, and returning the pickled child; a fresh session merges all
  children and commits. Verified. This is the shape Step Functions Distributed Map needs (the
  per-worker batching happens inside the map, so the coordinator cannot pre-make N forks).

This sub-project (A) graduates those mechanics into the `VirtualizarrProcessor` interface and a
synthetic reference implementation. It is **pure Python, no AWS**, fully testable locally.

## Scope

In scope:

- New backfill methods on the `VirtualizarrProcessor` Protocol (what a user implements).
- A generic framework module of fork/merge/promote helpers (identical for all users).
- A synthetic reference implementation of the new Protocol methods.
- Local pytest coverage of the full data-model round-trip.

Out of scope (later sub-projects):

- Lambda handlers, the partitioner, inventory handling, S3 fork serialization (sub-project B).
- CDK / Step Functions / Distributed Map orchestration, real S3 storage (sub-project C).
- Real-format VirtualiZarr parsing — the reference impl stays synthetic, matching the repo's
  existing `synthetic_vds` example; real parsing is the downstream user's responsibility.

The existing append-based methods (`initialize_repo`, `initialize_session`, `process_file`,
`commit_processed_files`, `garbage_collect`) are **untouched** — backfill is purely additive.

## Design decisions (from brainstorming)

1. **Architecture approved:** outer Step Function does Partitioner → Init(+commit) → serial Map
   over partitions (each: Fork → inner Distributed Map of workers → Reducer merge+commit) →
   Promote. (Full picture lives in B/C; A only builds the Processor-level pieces.)
2. **Partition/commit model:** serial partitions, **one commit per partition** (the issue's
   model) for incremental failure recovery.
3. **Full shape source:** the **user declares it explicitly** (constants/config). `region_for`
   and `initialize_backfill_store` assume a known global extent, independent of the inventory.
4. **File placement:** the **Processor computes each file's region from its file key**
   (deterministic), so the partitioner (B) can reuse the same logic to guarantee disjointness.
5. **Reference implementation:** **synthetic data**, mirroring the existing `synthetic_vds`
   example — fast local TDD, no fixtures or network.

## Components

### 1. Protocol additions — `virtualizarr_processor/typing.py`

These are the methods a user implements for backfill. Signatures use icechunk types already
imported in that module (`Repository`, `Session`) plus `ForkSession` and
`collections.abc.Mapping`.

- `initialize_backfill_store(self, repo: Repository) -> str`
  Create the `backfill` branch off the current `main` tip, build the **declared full-shape**
  array(s) and coordinate variables (metadata only — no data chunks yet), commit, and return the
  base snapshot id. The returned snapshot id is the clean base every fork descends from. Must
  leave the session with **no uncommitted changes** after committing (required for the
  fresh-session merge pattern).

- `region_for(self, file_key: str) -> Mapping[str, int]`
  Deterministically map a file key to its absolute index/region in the pre-sized array, as a
  per-dimension index map (e.g. `{"time": 42}`). Pure and side-effect-free so the partitioner
  can call it to assign and verify disjoint partitions.

- `process_backfill_file(self, file_key: str, fork: ForkSession) -> bool`
  Write the file's virtual references into `fork.store` at `self.region_for(file_key)` via
  `set_virtual_ref(...)`. Return `True` on success, `False` on parse/write failure (mirrors
  `process_file`'s bool contract). Must NOT commit (a `ForkSession` cannot commit).

### 2. Framework helpers — new `virtualizarr_processor/backfill.py`

Generic icechunk operations, identical for every user, NOT part of the Protocol (users must not
re-implement them):

- `create_fork(repo: Repository, branch: str = "backfill") -> bytes`
  Open a fresh `writable_session(branch)`, call `session.fork()`, return `pickle.dumps(fork)`.
  The session is clean (init already committed), so the fork's base is the branch-tip snapshot.

- `merge_and_commit(repo: Repository, child_fork_bytes: list[bytes], *, branch: str =
  "backfill", message: str) -> str`
  Open a fresh `writable_session(branch)`, `pickle.loads` each child fork,
  `session.merge(*children)`, `session.commit(message)`, return the new tip snapshot id. This is
  the verified fresh-session reducer pattern. (Uses `typing.cast(str, ...)` on the commit return
  — the pre-commit mypy hook runs without icechunk and would otherwise flag `Any`; see the spike
  findings.)

- `promote(repo: Repository, *, source: str = "backfill", target: str = "main") -> None`
  `repo.reset_branch(target, repo.lookup_branch(source))`.

Note: the worker loop (load shared fork → `fork.fork()` child → call `process_backfill_file` per
file → pickle child) lives in the worker **handler** in sub-project B, not here; A provides the
primitives it composes from.

### 3. Reference implementation — `virtualizarr_processor/processor.py`

Extend the existing `Processor` class with synthetic implementations of the three Protocol
methods, reusing the `synthetic_vds` / `BytesCodec` / `set_virtual_ref` patterns already proven
in the spike:

- `initialize_backfill_store`: declare a fixed full shape (module constants), create `backfill`
  off `main`, create the `foo` array at full shape with `serializer=BytesCodec(),
  compressors=None, filters=None`, write coordinate(s), commit, return the snapshot id.
- `region_for`: parse the synthetic file key into a `{"time": index}` map.
- `process_backfill_file`: write the synthetic chunk's virtual ref into the fork at the computed
  chunk key via `set_virtual_ref`.

## Data flow (exercised by the tests)

```
initialize_backfill_store(repo)            # backfill branch + full-shape store, COMMIT → base
        │
create_fork(repo)  → shared fork bytes
        │
  worker A: load shared fork → fork.fork() child_a → process_backfill_file(f, child_a) ... → pickle
  worker B: load shared fork → fork.fork() child_b → process_backfill_file(f, child_b) ... → pickle
        │  (real pickle round-trips, in-process; cross-process spawn already proven in the spike)
        ▼
merge_and_commit(repo, [child_a_bytes, child_b_bytes])   # fresh session, one commit
        │
promote(repo)                              # reset_branch main → backfill tip
        ▼
read-back: every slice resolves to its expected synthetic value on both backfill and main
```

## Error handling

- `process_backfill_file` returns `False` on failure rather than raising, matching `process_file`.
  The worker handler (B) decides batch-failure semantics.
- `merge_and_commit` lets commit errors propagate to the caller (the reducer in B), which owns
  retry/rebase policy.
- **Disjointness is assumed in A** (each test writes disjoint regions) and is the partitioner's
  responsibility in B — the spike confirmed `merge` does not detect overlapping-chunk conflicts
  (last-writer-wins).

## Testing

**Storage requirement (verified while planning):** the backfill tests must use
`icechunk.local_filesystem_storage(tmp_path)`, **not** the existing conftest's
`in_memory_storage()`. A pickled `ForkSession` cannot resolve its base snapshot from in-memory
storage even in the same process (`No data in memory found`), because the in-memory backing is
not carried across the pickle. Filesystem (and, in production, S3) storage is durable and shared,
so the reloaded fork resolves correctly. Consequently backfill requires durable storage — the
append-path `initialize_repo()` (which uses `in_memory_storage`) is left as-is, and backfill gets
its own filesystem-backed test fixture. `initialize_backfill_store(repo)` takes the repo as a
parameter, so it never dictates storage; the caller (test fixture here; the init Lambda in
sub-project B; S3 in production) chooses it.

Local pytest alongside the existing processor tests (`tests/`), following the spike's style:

- `region_for` is deterministic and returns the expected index for representative keys.
- `initialize_backfill_store` creates the `backfill` branch and a full-shape array (assert
  shape/dtype on a readonly session) and leaves no uncommitted changes.
- Full round-trip: init → `create_fork` → two workers (real pickle round-trip, `fork.fork()`
  children, `process_backfill_file`) → `merge_and_commit` → read-back all slices correct on
  `backfill`.
- `promote` → read-back all slices correct on `main`.
- A negative/characterization note is unnecessary here (covered by the spike); A assumes
  disjoint inputs.

## Deliverable

1. Protocol extended with the three backfill methods (documented).
2. `virtualizarr_processor/backfill.py` with `create_fork`, `merge_and_commit`, `promote`.
3. `Processor` reference implementation of the three methods (synthetic).
4. Passing local test suite covering the full data-model round-trip.
