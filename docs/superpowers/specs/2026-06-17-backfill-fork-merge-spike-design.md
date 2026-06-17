# Spike: Icechunk worker fork/merge with virtual references

**Date:** 2026-06-17
**Issue:** [#12 — Refactor backfill processing to use partitioned fork and merge approach](https://github.com/developmentseed/virtualizarr-data-pipelines/issues/12)
**Status:** Design approved, pending spec review

## Background

The pipeline currently ingests data by committing directly on `main`: SQS → `process_messages`
Lambda → one Icechunk commit per batch on `main`. This is fine for operational forward
processing, but for large backfills, scaling parallel workers creates a "thundering herd" of
rebase waits as workers contend for the `main` branch tip.

Issue #12 proposes a fork/merge backfill model: initialize the store at full shape on a
`backfill` branch, fan out many workers that each write a disjoint subset of chunk references
into independent Icechunk forks, then merge all forks into a single commit — maximizing the
writes-per-commit ratio and removing tip contention. Eventually this is orchestrated by AWS
Step Functions with a Distributed Map.

Before building any of that orchestration, we need to prove the **core Icechunk fork/merge
mechanics actually work** with VirtualiZarr virtual references and cross-process serialization.
That is the purpose of this spike.

## Goal

In a **local pytest harness** — with **real pickle round-trips** and **separate worker
processes** — prove the distributed fork → write → merge → commit cycle that issue #12 relies
on, before any Step Functions / Lambda / S3 / CDK work.

The spike is **disposable proof-of-mechanics code**. Its output is a passing test suite plus a
findings note that informs the real implementation design.

## Key facts established during design (icechunk 1.1.14, virtualizarr 2.2.1)

- `Session.fork()` → `ForkSession`; `Session.merge(*forks)`; `Session.commit(...)`. This is
  icechunk's documented distributed-write API.
- `ForkSession` is itself forkable (`ForkSession.fork()`) and mergeable.
- The icechunk docs describe the **coordinator** creating all forks from one
  `writable_session` and pickling them to workers. Whether workers can *independently* create
  their own forks and still merge is the central open question (see Risks).
- **VirtualiZarr `to_icechunk` only supports `append_dim`, not `region`.** Appends mutate array
  shape and would conflict across forks, so we cannot use it for disjoint writes.
- **`IcechunkStore.set_virtual_ref(key, location, *, offset, length, ...)`** (and batch
  `set_virtual_refs`) places a virtual chunk reference at a *specific chunk key* in a pre-sized
  array — no append, so parallel forks touch disjoint chunks and merge cleanly. This is the
  disjoint-write mechanism.
- Branch management: `repo.create_branch(name, snapshot_id)`, `repo.lookup_branch(name)` →
  tip snapshot id, `repo.reset_branch("main", tip)` for promotion.

## Scope decisions (from brainstorming)

- **Spike, not full build.** De-risk fork/merge first; defer Step Functions infra.
- **Local pytest, real pickle round-trip**, workers in separate processes (`multiprocessing`
  with `spawn` to force genuine pickling).
- **Straight to virtual references** — mirror the real pipeline rather than prototyping with
  plain real-data region writes first.
- **Workers only — no partition concept in the spike.** A worker gets a disjoint subset of
  chunk indices (the real pipeline will size these via `MaxItemsPerBatch`).
- **Each worker creates its own fork** and pickles the result to a shared folder. The
  coordinator does init up front, then merge + commit once all workers finish. The reducer
  **discovers forks by listing the folder** (mirrors a reducer listing an S3 prefix), not via
  return values.
- **Include `main` promotion** via `reset_branch`.

## Model

```
Coordinator                                    Workers (separate processes)
-----------                                    ----------------------------
init_backfill_store(repo, N)
  create branch "backfill" off main
  create array foo, shape (N, y, x),
    chunk (1, y, x), BytesCodec, fill_value
  write source chunk bytes to local file
  commit "Initialize backfill shape"
  │
  ├── split N chunk indices into W subsets
  ├── launch W worker processes ───────────►   open_repo(path)
  │                                            session = repo.writable_session("backfill")
  │                                            fork = session.fork()
  │                                            for t in subset:
  │                                              fork.store.set_virtual_ref(
  │                                                f"foo/c/{t}/0/0", location,
  │                                                offset=..., length=...,
  │                                                validate_container=False)
  │                                            pickle.dump(fork, forks/worker_{i}.pkl)
  │ wait for all workers
  ▼
  session = repo.writable_session("backfill")
  forks = [pickle.load(p) for p in list(forks/)]   ← discovery by folder listing
  session.merge(*forks)
  session.commit("Backfill commit")
  │
  ▼
  repo.reset_branch("main", repo.lookup_branch("backfill"))   ← promotion
```

## Components

All new code under `tests/spike/`. **No production code is touched.**

- **`tests/spike/backfill_spike.py`** — helpers:
  - `open_repo(path)` — open the repo from `icechunk.local_filesystem_storage(path)` with the
    virtual-chunk-container config + authorization (shared by coordinator and workers).
  - `init_backfill_store(repo, n_time)` — create `backfill` branch, create array `foo` at full
    shape with metadata only, write the shared source chunk bytes to a local file via
    `obstore`, commit. Returns nothing (state is in the repo).
  - `worker(path, indices, location, offset, length, out_path)` — the worker body, run in a
    child process: open repo, `writable_session("backfill")`, `fork()`, `set_virtual_ref` per
    index, pickle the fork to `out_path`.
  - `merge_and_commit(repo, forks_dir)` — open `writable_session("backfill")`, list/load every
    fork file in `forks_dir`, `merge(*forks)`, `commit(...)`, return the new tip snapshot id.
  - `promote(repo)` — `reset_branch("main", lookup_branch("backfill"))`.
- **`tests/spike/test_fork_merge.py`** — the assertions (below).

### Storage and data shape

- Repo storage: `icechunk.local_filesystem_storage(tmp_path)` so child processes can resolve
  the same repo (mirrors an S3-backed deployment; in-memory storage would not survive process
  boundaries).
- Source chunk bytes: a single local file written via `obstore`, referenced through a
  URL-prefixed `VirtualChunkContainer` — the same pattern the existing synthetic processor
  uses. Each `foo/c/{t}/0/0` chunk points at that file with an `(offset, length)`.
- Array `foo`: shape `(N, y, x)`, chunk `(1, y, x)`, `BytesCodec`, dims `(time, y, x)` — one
  chunk per time step so each chunk index maps cleanly to one worker write.

## Verification

The spike is a real test, not a demo. Assertions:

1. **Primary hypothesis — independently-created forks merge.** With workers each opening their
   own `writable_session` and forking, `merge(*forks)` + `commit()` succeeds.
2. **Read-back on `backfill`.** After commit, open `backfill` and assert every one of the `N`
   time slices resolves to the expected bytes/values, and no slice is missing/fill-valued.
3. **Promotion.** After `reset_branch`, `xr.open_zarr` on `main` returns the full `(N, y, x)`
   array, all slices correct.
4. **Negative control — conflict is detected.** Two forks writing the *same* chunk key, both
   dropped in the folder, cause `merge()` to raise. This proves disjointness is what makes the
   happy path pass (not luck), and that merge genuinely detects conflicts.

## Risks and the documented fallback

**Central risk:** the icechunk docs describe the *coordinator* creating all forks from one
session and pickling them to workers. In our preferred model each worker independently opens
its own `writable_session` and forks it. `merge` may track fork lineage by an internal id and
reject forks that did not descend from the merging session — or it may simply union change
sets that share the same base snapshot. Unknown until run.

**Decision (from brainstorming): test our model as the primary assertion, with a documented
fallback.** If assertion #1 fails (merge rejects independently-created forks):

- Record the exact failure (error type/message) in the findings note.
- Add a fallback test using the **coordinator-creates-forks** pattern: the coordinator opens
  one `writable_session`, calls `fork()` once per worker, pickles each fork out to the workers;
  workers write + pickle back; coordinator lists the folder and merges. This proves the cycle
  works *some* way and tells the real design which fork-ownership model to use.

Either outcome is a successful spike — it yields a definitive answer plus a working harness.

Secondary things the spike will surface (for the findings note):

- Whether `set_virtual_ref` requires the array metadata to pre-exist (expected yes) and whether
  `validate_container=False` is needed for the local virtual chunk container.
- Whether the nested lineage `session → fork → (worker writes)` round-trips through pickle
  intact across a `spawn`ed process.
- Any gotchas in opening the same `local_filesystem_storage` repo concurrently from multiple
  processes.

## Out of scope (deferred to the real implementation design)

- AWS Step Functions / Distributed Map orchestration.
- Lambda packaging and the init/worker/reducer handler split.
- Serializing forks to S3 (the spike uses a local folder as the stand-in).
- The partitioner over an inventory file / `MaxItemsPerBatch` sizing.
- CDK infrastructure changes.
- `VirtualizarrProcessor` Protocol redesign for region/ref writes.

The spike informs all of these but builds none of them.

## Deliverable

1. A passing `tests/spike/` suite implementing the model and all four assertions (plus the
   fallback path if the primary hypothesis fails).
2. A short **findings note** appended to this spec (what worked, the merge-lineage answer,
   gotchas, and a recommended shape for the future `VirtualizarrProcessor` interface change)
   to carry into the real implementation design.
