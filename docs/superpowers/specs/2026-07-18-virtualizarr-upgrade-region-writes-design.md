# VirtualiZarr/Icechunk upgrade + region-write backfill

**Date:** 2026-07-18
**Status:** Design approved, pending spec review
**Overview:** `docs/superpowers/backfill-pipeline-overview.md`
**Context:** Follows the completed backfill pipeline (A → B → C). This upgrades the
virtualizarr/icechunk/zarr/Python stack and switches the backfill write mechanism from manual
`set_virtual_ref` to the higher-level `vz.to_icechunk(store, region="auto")`, which is easier for
users (who copy the reference `Processor`) to understand.

## Background

The backfill pipeline writes disjoint regions of a pre-sized array by hand-constructing chunk keys
and calling `IcechunkStore.set_virtual_ref`. This was necessary because the pinned
**virtualizarr 2.2.1** `to_icechunk` only supported `append_dim`, not region writes.

**virtualizarr 2.7.1** adds `region` (and `mode`) to `to_icechunk`, so a per-file virtual dataset
can be written into a specific region of an existing array with one call. Verified during design:
`vds.vz.to_icechunk(fork.store, region="auto", validate_containers=False)` works on a **fork**
store with coordinate alignment (two forks wrote disjoint coordinates, merged, and read back
correct; the untouched slice stayed fill-valued). This confirms the region approach is compatible
with the distributed fork/merge model.

Upgrading virtualizarr to 2.7.1 also pulls **icechunk 1.1.14 → 2.1.1** (a major bump) and requires
**Python ≥3.12**. The fork/merge API still works on icechunk 2.x, but
`authorize_virtual_chunk_access={prefix: None}` is deprecated; the replacement is
`{prefix: icechunk.credentials.LocalFileSystemAccess}` (a sentinel, no parens) — both verified.

## Scope

Full stack upgrade + region writes, structured as two phases so the version bump and the mechanism
swap are de-risked independently. Unchanged: the sub-project B handler I/O contracts and the
sub-project C state machine / CDK tests (the change is internal to the reference `Processor` and the
mechanics tests). The operational **append path stays append-based** — only backfill moves to
region writes.

## Phase 1 — Upgrade the stack (no behavior change)

Goal: the full existing suite green on virtualizarr 2.7.1 / icechunk 2.1.1 / zarr ≥3.1.1 / Python
≥3.12, still using `set_virtual_ref` — behavior unchanged.

1. **Version bumps.** Set `requires-python = ">=3.12"` in all six `pyproject.toml` (root +
   `lambda/backfill`, `garbage_collect`, `initialize`, `process_messages`, `virtualizarr-processor`).
   Update the `virtualizarr-processor` package deps to require `virtualizarr>=2.7`,
   `icechunk>=2.1`, `zarr>=3.1.1`; add matching `icechunk>=2.1` where the lambda packages pin it.
   Regenerate `uv.lock` (`uv sync`).

2. **icechunk 2.x credential migration.** Replace `authorize_virtual_chunk_access={<prefix>: None}`
   with `{<prefix>: icechunk.credentials.LocalFileSystemAccess}` at all five sites:
   - `lambda/virtualizarr-processor/virtualizarr_processor/processor.py:86` (`initialize_repo`)
   - `lambda/virtualizarr-processor/virtualizarr_processor/processor.py:167` (`open_backfill_repo`)
   - `tests/conftest.py:74` (`create_repo`) and `tests/conftest.py:113` (`backfill_repo`)
   - `tests/backfill_mechanics/mechanics_harness.py:64` (`open_repo`)
   (The reference impl uses a local `file://` virtual chunk container, so `LocalFileSystemAccess`
   is the correct sentinel. Real users writing S3-backed virtual chunks would use an S3
   credential; that is the user's Processor concern, not the reference.)

3. **Surface-and-fix.** Run `uv run pytest`. Fix any remaining icechunk-2.x / virtualizarr-2.7 /
   zarr breakages. The two likely spots:
   - **GC path** — `processor.py:195-196` (`repo.expire_snapshots(older_than=...)`,
     `repo.garbage_collect(delete_object_older_than=...)`, `icechunk.GCSummary`) and its test
     `tests/test_example.py::test_garbage_collect`.
   - **Append path** — `process_file` uses `vds.vz.to_icechunk(session.store, append_dim="time")`;
     confirm `append_dim` still behaves without an explicit `mode` on 2.7.1 (the signature keeps
     `append_dim`; `mode` defaults to `None`).
   Any fix here is a mechanical API migration, not a behavior change.

At the end of Phase 1 the suite is green and `set_virtual_ref` is still in use.

## Phase 2 — Region-write refactor (backfill only)

Goal: replace `set_virtual_ref` with `vz.to_icechunk(store, region="auto")` in the backfill path;
suite green with the new mechanism.

### Reference `Processor` — `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`

- **`initialize_backfill_store`:** create the full-shape `foo` array `(BACKFILL_N, Y, X)` as now,
  **and write the full `time` coordinate** array (`np.arange(BACKFILL_N)`, int64, dims `("time",)`)
  so `region="auto"` can align each per-file write by coordinate. Commit; return the snapshot id.
  Remove the old single-source `BACKFILL_SOURCE_PATH`/`BACKFILL_SOURCE_URL` write (per-file writes
  now carry their own source).
- **`process_backfill_file(file_key, fork) -> bool`:** `t = self.region_for(file_key)["time"]`;
  build a per-file virtual dataset — shape `(1, Y, X)` `foo` filled with value `t`, its own
  synthetic source chunk (written to a per-index local file, referenced via a `ManifestArray`),
  carrying `time=[t]` as a coordinate; then
  `vds.vz.to_icechunk(fork.store, region="auto", validate_containers=False)`. Return `True`;
  on failure return `False` (unchanged bool contract). No `set_virtual_ref`.
- **`region_for`** unchanged (`{"time": int(file_key)}`) — now the source of the coordinate value
  the per-file vds carries.
- Constants: keep `BACKFILL_N/Y/X`, `BACKFILL_DTYPE`; remove `BACKFILL_CHUNK_NBYTES`,
  `BACKFILL_SOURCE_PATH`, `BACKFILL_SOURCE_URL` (no longer used).

### Protocol docstrings — `typing.py`

Update `initialize_backfill_store` (now writes coordinates) and `process_backfill_file` (writes a
per-file vds via `to_icechunk(region="auto")` rather than `set_virtual_ref`) to describe the new
mechanism.

### Mechanics — `tests/backfill_mechanics/`

- **`mechanics_harness.py`:** `init_backfill_store` writes the `time` coordinate; `run_worker`
  builds a per-index vds and calls `to_icechunk(fork.store, region="auto")` instead of
  `set_virtual_ref`; `open_repo` uses the new credential sentinel (from Phase 1). The cross-process
  `spawn` worker remains the target (mechanism swapped, structure intact).
- **`test_fork_merge_mechanics.py`:** the `test_cross_process_fork_merge_commits_all_slices` spawn
  test stays valid. The overlap-characterization test is rewritten: two forks write the **same**
  coordinate via `to_icechunk(region="auto")` with different data, then assert the observed merge
  outcome. (Whether icechunk 2.x still resolves this last-writer-wins is verified during
  implementation; the test asserts the actual behavior and documents it.)

### Tests that exercise `process_backfill_file` indirectly

`tests/test_backfill.py` (round-trip) and `tests/backfill_handlers/` (worker + end-to-end) keep
their structure and assertions: `initialize_backfill_store` now also writes coordinates, and
`process_backfill_file` writes value `t` at coordinate `t`, so `foo[t] == t` still holds. Any
setup that referenced the removed `BACKFILL_SOURCE_*` constants is updated.

## Data flow (backfill write, after Phase 2)

`initialize_backfill_store` → full-shape `foo` + `time=arange(N)` coordinate, committed →
`create_fork` → each worker: `region_for(file_key)` → build per-file vds with `time=[t]` →
`vds.vz.to_icechunk(fork.store, region="auto")` places it at the matching coordinate → pickle the
fork → `merge_and_commit` → read back correct. The higher-level call replaces the hand-built chunk
keys, which is the intended simplification.

## Error handling

`process_backfill_file` keeps its `try/except → bool` contract. Disjointness remains the operator's
responsibility (two files mapping to the same coordinate collide — the merge does not protect,
which the rewritten characterization test documents). No change to the state machine's failure
policy (C).

## Testing

- Phase 1: the entire existing suite passes on the upgraded stack with no assertion changes (only
  the credential migration + any surfaced API fixes).
- Phase 2: the backfill round-trip, handler, end-to-end, and mechanics tests pass with the
  region-write mechanism; `grep set_virtual_ref` returns nothing in `lambda/` and `tests/`.

## Deliverable

1. Upgraded stack (deps + `requires-python ≥3.12` + regenerated lock) with the icechunk-2.x
   migration; full suite green, behavior unchanged (Phase 1).
2. Backfill region-write refactor: `initialize_backfill_store` writes coordinates,
   `process_backfill_file` uses `to_icechunk(region="auto")`, mechanics tests converted,
   `set_virtual_ref` removed; full suite green (Phase 2).
