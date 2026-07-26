# VirtualiZarr/Icechunk Upgrade + Region-Write Backfill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the project to virtualizarr 2.7.1 / icechunk 2.1.1 / zarr ≥3.1.1 / Python ≥3.12, then switch the backfill write mechanism from manual `set_virtual_ref` to the higher-level `vz.to_icechunk(fork.store, region="auto")`.

**Architecture:** Two phases. Phase 1 bumps versions and migrates the one icechunk-2.x breaking change (the `authorize_virtual_chunk_access` credential sentinel), leaving behavior unchanged and the full suite green. Phase 2 rewrites the reference `Processor`'s backfill methods and the mechanics tests to use region writes (which require writing a `time` coordinate at init and carrying it on each per-file virtual dataset).

**Tech Stack:** virtualizarr 2.7.1, icechunk 2.1.1, zarr ≥3.1.1, Python ≥3.12, `uv`, `pytest`. Every mechanism below (region="auto" on a fork store, the credential sentinel, GC/append compatibility) was prototyped against these exact versions.

**Spec:** `docs/superpowers/specs/2026-07-18-virtualizarr-upgrade-region-writes-design.md`

---

## Critical implementation notes (verified while planning)

1. **icechunk 2.x credential:** `authorize_virtual_chunk_access={<prefix>: None}` →
   `{<prefix>: icechunk.credentials.LocalFileSystemAccess}` — a **sentinel class, no parentheses**
   (`LocalFileSystemAccess()` raises "object is not callable"). `{prefix: None}` still works but
   emits a DeprecationWarning.
2. **GC and append are API-compatible on 2.1.1** (verified): `repo.expire_snapshots(older_than=…)`,
   `repo.garbage_collect(delete_object_older_than=…) -> GCSummary`, and
   `vds.vz.to_icechunk(store, append_dim="time")` are unchanged. No code change needed there beyond
   the version bump.
3. **region="auto" needs a coordinate:** the array must carry a `time` coordinate (values the
   writes align to), and each per-file vds must carry its matching `time` coordinate value.
   Verified on a fork store: two forks wrote disjoint coordinates, merged, read back correct.
4. **isolated pre-commit mypy has no icechunk/virtualizarr** (imports → `Any`). Keep the
   `cast(str, …)` on `session.commit(…)` returns; `-> Repository` needs no cast. Test/handler funcs
   stay fully typed.

## File Structure

- **Modify all six `pyproject.toml`** (root + `lambda/{backfill,garbage_collect,initialize,process_messages,virtualizarr-processor}`) — `requires-python`, dep bumps, tool config.
- **Regenerate `uv.lock`.**
- **Modify `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`** — credentials (P1); `initialize_backfill_store` + `process_backfill_file` region-write rewrite, remove source constants (P2).
- **Modify `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`** — backfill docstrings (P2).
- **Modify `tests/conftest.py`** — credentials (P1).
- **Modify `tests/backfill_mechanics/mechanics_harness.py`** — credentials (P1); region-write rewrite (P2).
- **Modify `tests/backfill_mechanics/test_fork_merge_mechanics.py`** — region-write conversion (P2).
- **Modify `tests/test_backfill.py`** — assert the `time` coordinate at init (P2).

The B handler contracts and the C construct/CDK tests are untouched.

---

### Task 1 (Phase 1): Upgrade the stack + icechunk 2.x credential migration

**Files:**
- Modify: all six `pyproject.toml`, `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`, `tests/conftest.py`, `tests/backfill_mechanics/mechanics_harness.py`
- Regenerate: `uv.lock`

This is an atomic upgrade — the whole stack moves together. The "test" is the existing suite
passing on the new stack (no behavior change).

- [ ] **Step 1: Bump `requires-python` and tool config in the root `pyproject.toml`**

Change `requires-python = ">=3.11"` → `requires-python = ">=3.12"`. Under `[tool.ruff]` change
`target-version = "py311"` → `"py312"`. Under `[tool.mypy]` change `python_version = "3.11"` →
`"3.12"`.

- [ ] **Step 2: Bump `requires-python` in the five lambda pyprojects**

In each of `lambda/backfill/pyproject.toml`, `lambda/garbage_collect/pyproject.toml`,
`lambda/initialize/pyproject.toml`, `lambda/process_messages/pyproject.toml`,
`lambda/virtualizarr-processor/pyproject.toml`: change `requires-python = ">=3.11"` → `">=3.12"`.

- [ ] **Step 3: Bump the library dependencies**

In `lambda/virtualizarr-processor/pyproject.toml`, change the `dependencies` from
`["icechunk", "virtualizarr"]` to:

```toml
dependencies = [
    "icechunk>=2.1",
    "virtualizarr>=2.7",
    "zarr>=3.1.1",
]
```

In each lambda package that pins `icechunk` (`lambda/backfill/pyproject.toml`,
`lambda/process_messages/pyproject.toml`, and any other with a bare `"icechunk"`), change it to
`"icechunk>=2.1"`.

- [ ] **Step 4: Migrate the credential sentinel (5 sites)**

Replace `authorize_virtual_chunk_access={<prefix>: None}` with
`authorize_virtual_chunk_access={<prefix>: icechunk.credentials.LocalFileSystemAccess}` at:
- `lambda/virtualizarr-processor/virtualizarr_processor/processor.py` — the `initialize_repo`
  call (`{CHUNK_DIRECTORY_URL_PREFIX: None}`) and the `open_backfill_repo` call
  (`{CHUNK_DIRECTORY_URL_PREFIX: None}`).
- `tests/conftest.py` — both `create_repo` and the `backfill_repo` fixture
  (`{CHUNK_DIRECTORY_URL_PREFIX: None}`).
- `tests/backfill_mechanics/mechanics_harness.py` — `open_repo` (`{_url_prefix(work): None}`).

Example (processor.py):

```python
        return icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access={
                CHUNK_DIRECTORY_URL_PREFIX: icechunk.credentials.LocalFileSystemAccess
            },
        )
```

- [ ] **Step 5: Regenerate the lock and install**

Run: `uv sync`
Expected: resolves virtualizarr 2.7.1, icechunk 2.1.1, zarr ≥3.1.1 (and their deps); updates
`uv.lock`. If the local interpreter is <3.12, use `uv sync -p 3.12` (the dev env in this repo is
already 3.13).

- [ ] **Step 6: Run the whole suite — must be green with no behavior change**

Run: `uv run pytest -q`
Expected: all pass (35). The credential migration removes the deprecation warning; GC, append,
fork/merge, and the synthetic-vds construction are all API-compatible on the new stack (verified).
If something fails, it is a real 2.x/2.7 migration surprise — fix it minimally (mechanical API
change only, no behavior change) and note it.

- [ ] **Step 7: Confirm no lingering deprecated credential form**

Run: `grep -rn "authorize_virtual_chunk_access" tests lambda | grep ": None}"` → expect no matches.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml lambda/*/pyproject.toml uv.lock lambda/virtualizarr-processor/virtualizarr_processor/processor.py tests/conftest.py tests/backfill_mechanics/mechanics_harness.py
git commit -m "build: upgrade to virtualizarr 2.7.1 / icechunk 2.1.1 / zarr / py3.12 + credential migration"
```

---

### Task 2 (Phase 2): `initialize_backfill_store` writes the `time` coordinate

`region="auto"` aligns each write by coordinate, so the full-shape store must carry the `time`
coordinate.

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`
- Test: `tests/test_backfill.py`

- [ ] **Step 1: Add a coordinate assertion to the init test**

In `tests/test_backfill.py`, in `test_initialize_backfill_store_creates_full_shape`, after the
existing `foo` assertions, add:

```python
    import numpy as np

    time_coord = zarr.open_group(session.store, mode="r")["time"]
    assert time_coord.shape == (6,)
    assert (np.asarray(time_coord[:]) == np.arange(6)).all()
```

(`session` here is the readonly backfill session already opened in that test; `np` may already be
imported at the top of the file — if so, drop the inline import.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_backfill.py::test_initialize_backfill_store_creates_full_shape -v`
Expected: FAIL — `time` array does not exist yet (KeyError).

- [ ] **Step 3: Write the coordinate in `initialize_backfill_store`**

In `processor.py`, `initialize_backfill_store`, after `root.create_array("foo", ...)` and before
the commit, add the `time` coordinate:

```python
        time_coord = root.create_array(
            "time",
            shape=(BACKFILL_N,),
            chunks=(BACKFILL_N,),
            dtype="int64",
            dimension_names=("time",),
        )
        time_coord[:] = np.arange(BACKFILL_N)
```

(`root` is the `zarr.open_group(session.store, mode="a")` already created in that method; `np` is
already imported in `processor.py`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_backfill.py::test_initialize_backfill_store_creates_full_shape -v`
Expected: PASS.

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -q`
Expected: all pass (the round-trip still uses `set_virtual_ref`, which is unaffected by adding a
coordinate).

- [ ] **Step 6: Commit**

```bash
git add lambda/virtualizarr-processor/virtualizarr_processor/processor.py tests/test_backfill.py
git commit -m "feat: initialize_backfill_store writes the time coordinate for region=auto"
```

---

### Task 3 (Phase 2): `process_backfill_file` uses `to_icechunk(region="auto")`

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`, `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`

The round-trip test (`tests/test_backfill.py::test_full_backfill_round_trip`) already asserts
`foo[t] == t` for all slices; it is the behavioral gate for this refactor.

- [ ] **Step 1: Confirm the round-trip currently passes (baseline)**

Run: `uv run pytest tests/test_backfill.py::test_full_backfill_round_trip -v`
Expected: PASS (still on `set_virtual_ref`).

- [ ] **Step 2: Rewrite `process_backfill_file` and drop the source constants**

In `processor.py`:

(a) Remove the now-unused constants `BACKFILL_CHUNK_NBYTES`, `BACKFILL_SOURCE_PATH`,
`BACKFILL_SOURCE_URL` (keep `BACKFILL_N`, `BACKFILL_Y`, `BACKFILL_X`, `BACKFILL_DTYPE`).

(b) Replace the body of `process_backfill_file` with a per-file virtual dataset + region write:

```python
    def _backfill_slice_vds(self, t: int) -> xr.Dataset:
        """A one-time-step virtual dataset for backfill index t, carrying the
        matching `time` coordinate so to_icechunk(region="auto") can place it."""
        buf = np.full((1, BACKFILL_Y, BACKFILL_X), t, dtype=BACKFILL_DTYPE).tobytes()
        filepath = f"{CHUNK_DIR}/backfill_slice_{t}"
        obstore.put(obstore.store.LocalStore(), filepath, buf)
        manifest = ChunkManifest(
            {"0.0.0": {"path": filepath, "offset": 0, "length": len(buf)}}
        )
        zdtype = parse_data_type(BACKFILL_DTYPE, zarr_format=3)
        metadata = ArrayV3Metadata(
            shape=(1, BACKFILL_Y, BACKFILL_X),
            data_type=zdtype,
            chunk_grid={
                "name": "regular",
                "configuration": {"chunk_shape": (1, BACKFILL_Y, BACKFILL_X)},
            },
            chunk_key_encoding={"name": "default"},
            fill_value=zdtype.default_scalar(),
            codecs=[BytesCodec()],
            attributes={},
            dimension_names=("time", "y", "x"),
            storage_transformers=None,
        )
        ma = ManifestArray(chunkmanifest=manifest, metadata=metadata)
        return xr.Dataset(
            {"foo": xr.Variable(("time", "y", "x"), ma)},
            coords={"time": ("time", [t])},
        )

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        try:
            t = self.region_for(file_key)["time"]
            self._backfill_slice_vds(t).vz.to_icechunk(
                fork.store, region="auto", validate_containers=False
            )
            return True
        except Exception:
            # Catch parse/region errors and I/O failures from to_icechunk.
            return False
```

All names used (`np`, `obstore`, `ChunkManifest`, `ManifestArray`, `ArrayV3Metadata`,
`parse_data_type`, `BytesCodec`, `xr`, `CHUNK_DIR`, `ForkSession`) are already imported in
`processor.py`.

- [ ] **Step 3: Run the round-trip + full suite**

Run: `uv run pytest tests/test_backfill.py -v`
Expected: PASS — the round-trip verifies `foo[t] == t` for all slices via the region write.

Run: `uv run pytest -q`
Expected: all pass (the handler + end-to-end tests exercise `process_backfill_file` through the
handlers and are unaffected by the internal mechanism change).

- [ ] **Step 4: Update the Protocol docstrings in `typing.py`**

In `typing.py`, update `initialize_backfill_store`'s docstring to mention it also writes the
coordinate(s) the region writes align to, and update `process_backfill_file`'s docstring to say it
writes a per-file virtual dataset into the store's region via
`vz.to_icechunk(store, region="auto")` (rather than `set_virtual_ref`). Keep the "must not commit"
and bool-contract notes.

- [ ] **Step 5: Confirm `set_virtual_ref` is gone from the processor**

Run: `grep -rn "set_virtual_ref" lambda` → expect no matches.

- [ ] **Step 6: Commit**

```bash
git add lambda/virtualizarr-processor/virtualizarr_processor/processor.py lambda/virtualizarr-processor/virtualizarr_processor/typing.py
git commit -m "feat: process_backfill_file uses vz.to_icechunk(region=auto) instead of set_virtual_ref"
```

---

### Task 4 (Phase 2): Convert the mechanics harness + tests to region writes

**Files:**
- Modify: `tests/backfill_mechanics/mechanics_harness.py`, `tests/backfill_mechanics/test_fork_merge_mechanics.py`

- [ ] **Step 1: Rewrite the harness to region-write**

In `tests/backfill_mechanics/mechanics_harness.py`:

(a) `init_backfill_store(repo, work)` — after creating the `foo` array, also write the `time`
coordinate (mirroring the processor):

```python
    time_coord = root.create_array(
        "time", shape=(N,), chunks=(N,), dtype="int64", dimension_names=("time",)
    )
    time_coord[:] = np.arange(N)
```

(b) Replace `run_worker` (which used `set_virtual_ref`) with a per-index region write. Add
imports at the top (`import xarray as xr`, plus the manifest/metadata imports already present or
add them: `from virtualizarr.manifests import ChunkManifest, ManifestArray`,
`from zarr.core.dtype import parse_data_type`, `from zarr.core.metadata import ArrayV3Metadata`).
New body:

```python
def _slice_vds(work: str, t: int) -> "xr.Dataset":
    buf = np.full((1, Y, X), t, dtype=DTYPE).tobytes()
    path = f"{_chunks_dir(work)}/slice_{t}"
    obstore.put(obstore.store.LocalStore(), path, buf)
    manifest = ChunkManifest({"0.0.0": {"path": path, "offset": 0, "length": len(buf)}})
    zdtype = parse_data_type(DTYPE, zarr_format=3)
    metadata = ArrayV3Metadata(
        shape=(1, Y, X),
        data_type=zdtype,
        chunk_grid={"name": "regular", "configuration": {"chunk_shape": (1, Y, X)}},
        chunk_key_encoding={"name": "default"},
        fill_value=zdtype.default_scalar(),
        codecs=[BytesCodec()],
        attributes={},
        dimension_names=("time", "y", "x"),
        storage_transformers=None,
    )
    ma = ManifestArray(chunkmanifest=manifest, metadata=metadata)
    return xr.Dataset(
        {"foo": xr.Variable(("time", "y", "x"), ma)}, coords={"time": ("time", [t])}
    )


def run_worker(in_path: str, indices: list[int], work: str, out_path: str) -> None:
    """Load the coordinator-made fork, region-write each assigned index via
    to_icechunk(region="auto"), pickle the fork back. Runs in a spawned process."""
    with open(in_path, "rb") as f:
        fork = pickle.loads(f.read())
    for t in indices:
        _slice_vds(work, t).vz.to_icechunk(
            fork.store, region="auto", validate_containers=False
        )
    with open(out_path, "wb") as f:
        f.write(pickle.dumps(fork))
```

Note the signature change: `run_worker` now takes `work` (the working dir) instead of the old
`source_url`. In `run_backfill`, change the spawn args from
`args=(in_path, subset, _source_url(work), out_path)` to `args=(in_path, subset, work, out_path)`.
Remove the now-unused `write_source`, `_source_path`, `_source_url` helpers (each `_slice_vds`
writes its own source chunk). Add `import virtualizarr` at the top so the `.vz` accessor registers
(needed in the spawned child too).

- [ ] **Step 2: Convert the mechanics tests**

In `tests/backfill_mechanics/test_fork_merge_mechanics.py`:

(a) `test_cross_process_fork_merge_commits_all_slices` — it calls `mh.run_backfill(repo, work,
subsets=...)`, which still drives the (rewritten) `run_worker`; no change needed beyond removing
any now-gone `mh.write_source(work)` call and confirming it still asserts every slice equals its
index.

(b) Rewrite `test_overlapping_writes_last_writer_wins_no_conflict` to use region writes: two forks
each region-write the **same** index with different data, merge, and assert the observed outcome:

```python
def test_overlapping_writes_last_writer_wins_no_conflict(
    tmp_path: pathlib.Path,
) -> None:
    work = str(tmp_path)
    repo = mh.open_repo(work)
    mh.init_backfill_store(repo, work)

    session = repo.writable_session("backfill")
    fork_a = session.fork()
    mh._slice_vds(work, 0).vz.to_icechunk(
        fork_a.store, region="auto", validate_containers=False
    )
    fork_b = session.fork()
    # same index 0, but value 5
    mh._slice_vds_value(work, index=0, value=5).vz.to_icechunk(
        fork_b.store, region="auto", validate_containers=False
    )

    session.merge(fork_a, fork_b)
    session.commit("overlapping region writes")

    arr = zarr.open_group(repo.readonly_session("backfill").store, mode="r")["foo"]
    # Assert whichever value actually wins (verify empirically; document the result).
    assert (np.asarray(arr[0]) == 5).all()
```

For this you need a `_slice_vds_value(work, index, value)` variant in the harness (same as
`_slice_vds` but the buffer is filled with `value` while the coordinate stays `index`). Add it
alongside `_slice_vds`. IMPORTANT: run this test and confirm the actual winning value; if icechunk
2.x does not resolve overlapping region writes as last-writer-wins (value 5), change the assertion
to match the observed behavior and note it — the test documents the real merge semantics.

- [ ] **Step 3: Run the mechanics tests**

Run: `uv run pytest tests/backfill_mechanics/ -v`
Expected: both PASS (the cross-process spawn write + the overlap characterization).

- [ ] **Step 4: Run the whole suite and confirm no `set_virtual_ref` remains**

Run: `uv run pytest -q`
Expected: all pass.

Run: `grep -rn "set_virtual_ref" tests lambda` → expect no matches.

- [ ] **Step 5: Commit**

```bash
git add tests/backfill_mechanics/mechanics_harness.py tests/backfill_mechanics/test_fork_merge_mechanics.py
git commit -m "test: convert backfill mechanics to vz.to_icechunk(region=auto)"
```

---

## Notes for the implementer

- The region="auto" fork-store write, the credential sentinel, and GC/append compatibility were
  all prototyped on virtualizarr 2.7.1 / icechunk 2.1.1. If a step diverges, that is a finding —
  report it.
- Phase 1 must land green with NO behavior change before starting Phase 2. Do not mix them.
- The append path (`process_file`) stays append-based — do not convert it to region writes.
- The overlap-characterization test asserts real observed merge behavior; verify the winning value
  rather than assuming.
- Handler contracts (B) and the CDK construct/tests (C) are untouched; the full suite (including
  the CDK synth tests) must stay green throughout.
