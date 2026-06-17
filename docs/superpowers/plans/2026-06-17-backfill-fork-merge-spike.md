# Icechunk fork/merge backfill spike — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove, in a local pytest harness with real cross-process pickling, that Icechunk's coordinator-creates-forks distributed-write cycle works with VirtualiZarr-style virtual chunk references — initialize a full-shape store on a `backfill` branch, fan forks out to worker processes that write disjoint chunk refs, merge all forks into one commit, and promote `main`.

**Architecture:** A single disposable helper module (`tests/spike/backfill_spike.py`) holds the coordinator/worker functions; a test module (`tests/spike/test_fork_merge.py`) drives them and asserts behavior. The coordinator opens one `writable_session("backfill")`, calls `session.fork()` once per worker, and pickles each fork to a `forks_in/` folder. Worker processes (spawned via `multiprocessing`, forcing genuine pickling) load their fork, call `IcechunkStore.set_virtual_ref` for their disjoint chunk-index subset, and pickle the fork to a `forks_out/` folder — without opening the repo. The coordinator discovers the returned forks by **listing** `forks_out/`, merges them into the same session, and commits once. Promotion fast-forwards `main` via `reset_branch`.

**Tech Stack:** Python 3.11+, `uv`, `pytest`, `icechunk` 1.1.14, `zarr` 3.1.5, `obstore`, `numpy`, `multiprocessing` (spawn). All mechanics in this plan were verified end-to-end against these versions while authoring the spec.

**Spec:** `docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md`

---

## File Structure

- **Create `tests/spike/backfill_spike.py`** — disposable spike helpers: constants, path helpers, `open_repo`, `write_source`, `init_backfill_store`, `run_worker` (module-level so it is importable by spawned children), `run_backfill` (coordinator orchestration), `promote`.
- **Create `tests/spike/test_fork_merge.py`** — the pytest assertions driving the helpers.

**Do NOT add `tests/spike/__init__.py`.** With pytest's default `prepend` import mode and no `__init__.py`, pytest puts `tests/spike/` on `sys.path`, so both modules import as top-level (`import backfill_spike`). `multiprocessing` `spawn` propagates the parent's `sys.path` to the child, which then re-imports `backfill_spike` by name. Adding an `__init__.py` would turn it into a package (`tests.spike.backfill_spike`) and break the spawned child's import. This exact layout was verified working.

No production code under `cdk/` or `lambda/` is touched.

---

### Task 1: Spike package scaffold + repo opener

**Files:**
- Create: `tests/spike/backfill_spike.py`
- Test: `tests/spike/test_fork_merge.py`

- [ ] **Step 1: Write the failing test**

Create `tests/spike/test_fork_merge.py`:

```python
import os
import sys

# tests/spike is on sys.path under pytest's default prepend import mode (no __init__.py),
# so backfill_spike imports as a top-level module — required for multiprocessing spawn.
sys.path.insert(0, os.path.dirname(__file__))

import backfill_spike as bk


def test_open_repo_creates_main_branch(tmp_path):
    repo = bk.open_repo(str(tmp_path))
    assert "main" in repo.list_branches()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_open_repo_creates_main_branch -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_spike'`.

- [ ] **Step 3: Write minimal implementation**

Create `tests/spike/backfill_spike.py`:

```python
"""Disposable spike: Icechunk coordinator-creates-forks distributed-write cycle.

See docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md.
This is throwaway proof-of-mechanics code, not production code.
"""

import os

import icechunk
import numpy as np

# Synthetic array: N time steps, each a (Y, X) int32 chunk. One chunk per time step.
N, Y, X = 6, 2, 3
DTYPE = np.dtype("int32")
CHUNK_NBYTES = Y * X * DTYPE.itemsize


def _chunks_dir(work: str) -> str:
    return os.path.join(work, "chunks")


def _repo_dir(work: str) -> str:
    return os.path.join(work, "repo")


def _url_prefix(work: str) -> str:
    return f"file://{_chunks_dir(work)}/"


def _source_path(work: str) -> str:
    return f"{_chunks_dir(work)}/source.bin"


def _source_url(work: str) -> str:
    return f"file://{_source_path(work)}"


def open_repo(work: str) -> icechunk.Repository:
    """Open (or create) the spike repo on local-filesystem storage with a virtual
    chunk container authorizing the local source file."""
    os.makedirs(_chunks_dir(work), exist_ok=True)
    chunk_store = icechunk.local_filesystem_store(_chunks_dir(work))
    storage = icechunk.local_filesystem_storage(_repo_dir(work))
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(_url_prefix(work), chunk_store)
    )
    return icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access={_url_prefix(work): None},
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_open_repo_creates_main_branch -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/spike/backfill_spike.py tests/spike/test_fork_merge.py
git commit -m "spike: repo opener for fork/merge backfill"
```

---

### Task 2: Source data + full-shape store initialization

**Files:**
- Modify: `tests/spike/backfill_spike.py`
- Test: `tests/spike/test_fork_merge.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/spike/test_fork_merge.py`:

```python
import zarr


def test_init_creates_backfill_branch_and_full_shape_array(tmp_path):
    work = str(tmp_path)
    repo = bk.open_repo(work)
    bk.write_source(work)
    bk.init_backfill_store(repo, work)

    assert "backfill" in repo.list_branches()
    session = repo.readonly_session("backfill")
    arr = zarr.open_group(session.store, mode="r")["foo"]
    assert arr.shape == (bk.N, bk.Y, bk.X)
    assert arr.dtype == bk.DTYPE
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_init_creates_backfill_branch_and_full_shape_array -v`
Expected: FAIL with `AttributeError: module 'backfill_spike' has no attribute 'write_source'`.

- [ ] **Step 3: Write minimal implementation**

Append to `tests/spike/backfill_spike.py`:

```python
import obstore
import zarr
from zarr.codecs import BytesCodec


def write_source(work: str) -> None:
    """Write one source file holding N back-to-back chunk buffers; chunk t is filled
    with the value t, so chunk t lives at byte offset t * CHUNK_NBYTES."""
    buf = b"".join(np.full((Y, X), t, dtype=DTYPE).tobytes() for t in range(N))
    obstore.put(obstore.store.LocalStore(), _source_path(work), buf)


def init_backfill_store(repo: icechunk.Repository, work: str) -> None:
    """Create the `backfill` branch off main and the full-shape `foo` array
    (metadata only — no chunks written yet)."""
    repo.create_branch("backfill", repo.lookup_branch("main"))
    session = repo.writable_session("backfill")
    root = zarr.open_group(session.store, mode="a")
    root.create_array(
        "foo",
        shape=(N, Y, X),
        chunks=(1, Y, X),
        dtype=DTYPE,
        serializer=BytesCodec(),
        compressors=None,
        filters=None,
        dimension_names=("time", "y", "x"),
    )
    session.commit("Initialize backfill shape")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_init_creates_backfill_branch_and_full_shape_array -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/spike/backfill_spike.py tests/spike/test_fork_merge.py
git commit -m "spike: source data and full-shape backfill store init"
```

---

### Task 3: Worker writes virtual refs (in-process round-trip)

Proves a pickled fork can receive `set_virtual_ref` writes and merge back into the originating session, before adding cross-process complexity.

**Files:**
- Modify: `tests/spike/backfill_spike.py`
- Test: `tests/spike/test_fork_merge.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/spike/test_fork_merge.py`:

```python
import pickle

import numpy as np


def test_worker_writes_refs_in_process(tmp_path):
    work = str(tmp_path)
    repo = bk.open_repo(work)
    bk.write_source(work)
    bk.init_backfill_store(repo, work)

    session = repo.writable_session("backfill")
    in_path = tmp_path / "fork_in.pkl"
    out_path = tmp_path / "fork_out.pkl"
    in_path.write_bytes(pickle.dumps(session.fork()))

    bk.run_worker(str(in_path), [0, 1, 2], bk._source_url(work), str(out_path))

    returned = pickle.loads(out_path.read_bytes())
    session.merge(returned)
    session.commit("partial backfill")

    arr = zarr.open_group(repo.readonly_session("backfill").store, mode="r")["foo"]
    for t in [0, 1, 2]:
        assert (np.asarray(arr[t]) == t).all()
    # Indices not written remain fill (0).
    assert (np.asarray(arr[4]) == 0).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_worker_writes_refs_in_process -v`
Expected: FAIL with `AttributeError: module 'backfill_spike' has no attribute 'run_worker'`.

- [ ] **Step 3: Write minimal implementation**

Append to `tests/spike/backfill_spike.py`:

```python
import pickle


def run_worker(in_path: str, indices: list[int], source_url: str, out_path: str) -> None:
    """Worker body. Runs in a separate (spawned) process. Loads the coordinator-made
    fork, writes a virtual chunk reference for each assigned time index, and pickles
    the fork back. Does NOT open the repo — the pickled fork carries everything."""
    with open(in_path, "rb") as f:
        fork = pickle.loads(f.read())
    for t in indices:
        fork.store.set_virtual_ref(
            f"foo/c/{t}/0/0",
            source_url,
            offset=t * CHUNK_NBYTES,
            length=CHUNK_NBYTES,
            validate_container=False,
        )
    with open(out_path, "wb") as f:
        f.write(pickle.dumps(fork))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_worker_writes_refs_in_process -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/spike/backfill_spike.py tests/spike/test_fork_merge.py
git commit -m "spike: worker set_virtual_ref + merge (in-process)"
```

---

### Task 4: Cross-process coordinator orchestration

The core proof: coordinator forks per worker → spawns real worker processes → discovers returned forks by listing the folder → merges all → single commit.

**Files:**
- Modify: `tests/spike/backfill_spike.py`
- Test: `tests/spike/test_fork_merge.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/spike/test_fork_merge.py`:

```python
def test_cross_process_fork_merge_commits_all_slices(tmp_path):
    work = str(tmp_path)
    repo = bk.open_repo(work)
    bk.write_source(work)
    bk.init_backfill_store(repo, work)

    tip = bk.run_backfill(repo, work, subsets=[[0, 1, 2], [3, 4, 5]])
    assert tip  # non-empty snapshot id

    arr = zarr.open_group(repo.readonly_session("backfill").store, mode="r")["foo"]
    for t in range(bk.N):
        assert (np.asarray(arr[t]) == t).all(), (t, np.asarray(arr[t]))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_cross_process_fork_merge_commits_all_slices -v`
Expected: FAIL with `AttributeError: module 'backfill_spike' has no attribute 'run_backfill'`.

- [ ] **Step 3: Write minimal implementation**

Append to `tests/spike/backfill_spike.py`:

```python
import multiprocessing as mp


def run_backfill(repo: icechunk.Repository, work: str, subsets: list[list[int]]) -> str:
    """Coordinator. Opens one writable session, forks once per worker subset, spawns
    a worker process per fork, then discovers the returned forks by listing the output
    folder, merges them into the same session, and commits once. Returns the new tip."""
    forks_in = os.path.join(work, "forks_in")
    forks_out = os.path.join(work, "forks_out")
    os.makedirs(forks_in, exist_ok=True)
    os.makedirs(forks_out, exist_ok=True)

    session = repo.writable_session("backfill")
    ctx = mp.get_context("spawn")
    procs = []
    for i, subset in enumerate(subsets):
        in_path = os.path.join(forks_in, f"worker_{i}.pkl")
        out_path = os.path.join(forks_out, f"worker_{i}.pkl")
        with open(in_path, "wb") as f:
            f.write(pickle.dumps(session.fork()))
        proc = ctx.Process(
            target=run_worker,
            args=(in_path, subset, _source_url(work), out_path),
        )
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()
        if proc.exitcode != 0:
            raise RuntimeError(f"worker exited with {proc.exitcode}")

    # Discovery by folder listing — mirrors a reducer listing an S3 prefix.
    forks = []
    for name in sorted(os.listdir(forks_out)):
        with open(os.path.join(forks_out, name), "rb") as f:
            forks.append(pickle.loads(f.read()))

    session.merge(*forks)
    return session.commit("Backfill commit")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_cross_process_fork_merge_commits_all_slices -v`
Expected: PASS, with `CROSS-PROCESS` data correct for all 6 slices.

- [ ] **Step 5: Commit**

```bash
git add tests/spike/backfill_spike.py tests/spike/test_fork_merge.py
git commit -m "spike: cross-process coordinator fork/merge orchestration"
```

---

### Task 5: Promote main to backfill tip

**Files:**
- Modify: `tests/spike/backfill_spike.py`
- Test: `tests/spike/test_fork_merge.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/spike/test_fork_merge.py`:

```python
def test_promotion_makes_backfill_visible_on_main(tmp_path):
    work = str(tmp_path)
    repo = bk.open_repo(work)
    bk.write_source(work)
    bk.init_backfill_store(repo, work)
    bk.run_backfill(repo, work, subsets=[[0, 1, 2], [3, 4, 5]])

    bk.promote(repo)

    arr = zarr.open_group(repo.readonly_session("main").store, mode="r")["foo"]
    expected = np.arange(bk.N)[:, None, None]  # each slice t == t
    assert (np.asarray(arr[:]) == expected).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_promotion_makes_backfill_visible_on_main -v`
Expected: FAIL with `AttributeError: module 'backfill_spike' has no attribute 'promote'`.

- [ ] **Step 3: Write minimal implementation**

Append to `tests/spike/backfill_spike.py`:

```python
def promote(repo: icechunk.Repository) -> None:
    """Fast-forward main to the current backfill tip."""
    repo.reset_branch("main", repo.lookup_branch("backfill"))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_promotion_makes_backfill_visible_on_main -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/spike/backfill_spike.py tests/spike/test_fork_merge.py
git commit -m "spike: promote main to backfill tip"
```

---

### Task 6: Characterization — overlapping writes are NOT detected

Documents (as an executable assertion) that `merge` performs no chunk-overlap conflict detection: two forks writing different content to the same chunk key commit silently, last-writer-wins. This is why disjointness must be guaranteed by the partitioner. Verified during spec authoring.

**Files:**
- Test: `tests/spike/test_fork_merge.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/spike/test_fork_merge.py`:

```python
def test_overlapping_writes_last_writer_wins_no_conflict(tmp_path):
    work = str(tmp_path)
    repo = bk.open_repo(work)
    bk.write_source(work)
    bk.init_backfill_store(repo, work)

    session = repo.writable_session("backfill")

    # Two forks BOTH target chunk key foo/c/0/0/0, but point at different source
    # offsets: fork A -> value 0, fork B -> value 5.
    fork_a = session.fork()
    fork_a.store.set_virtual_ref(
        "foo/c/0/0/0", bk._source_url(work),
        offset=0 * bk.CHUNK_NBYTES, length=bk.CHUNK_NBYTES, validate_container=False,
    )
    fork_b = session.fork()
    fork_b.store.set_virtual_ref(
        "foo/c/0/0/0", bk._source_url(work),
        offset=5 * bk.CHUNK_NBYTES, length=bk.CHUNK_NBYTES, validate_container=False,
    )

    # No conflict raised on merge or commit.
    session.merge(fork_a, fork_b)
    session.commit("overlapping writes")

    arr = zarr.open_group(repo.readonly_session("backfill").store, mode="r")["foo"]
    # Last fork merged (B) wins.
    assert (np.asarray(arr[0]) == 5).all()
```

- [ ] **Step 2: Run test to verify it passes immediately (no implementation change)**

Run: `uv run pytest tests/spike/test_fork_merge.py::test_overlapping_writes_last_writer_wins_no_conflict -v`
Expected: PASS. (This is a characterization test of existing icechunk behavior; if it instead raises on merge/commit, that is a behavior change worth recording in the findings note — update the assertion to match and document it.)

- [ ] **Step 3: Commit**

```bash
git add tests/spike/test_fork_merge.py
git commit -m "spike: characterize merge overlap (last-writer-wins, no conflict)"
```

---

### Task 7: Run the full suite and write the findings note

**Files:**
- Modify: `docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md`

- [ ] **Step 1: Run the entire spike suite**

Run: `uv run pytest tests/spike/ -v`
Expected: all 6 tests PASS.

- [ ] **Step 2: Append the findings note to the spec**

Append a `## Findings` section to `docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md` capturing the actual results. Fill in each bullet from the test run — do not leave it generic:

```markdown
## Findings (spike results)

- **Coordinator-creates-forks cycle works end-to-end.** A `writable_session` fork
  survives pickle → spawned worker process → `set_virtual_ref` writes → pickle-back →
  discovery-by-folder-listing → `merge(*forks)` → `commit`, and all N slices read back
  correct. `reset_branch("main", backfill_tip)` makes the data visible on `main`.
- **Workers need only the fork.** The spawned worker calls `set_virtual_ref` on the
  unpickled fork without opening the repo or holding storage config. Implication for the
  real pipeline: the worker Lambda needs only the pickled fork bytes plus the source
  location/offset/length — not repo credentials for writing.
- **Merge does not detect chunk overlap (last-writer-wins).** Confirmed by
  `test_overlapping_writes_last_writer_wins_no_conflict`. The partitioner MUST guarantee
  disjoint chunk assignments per worker; the merge provides no safety net.
- **Array creation:** `zarr.Group.create_array(..., serializer=BytesCodec(),
  compressors=None, filters=None)` on the session store produces a metadata-only array
  whose chunks are then populated by `set_virtual_ref`. Raw little-endian bytes via
  `BytesCodec` line up with the synthetic source file.
- **Storage caveat:** `local_filesystem_storage` warns it is unsafe for concurrent
  commits; not hit here because only the coordinator commits. The real deployment uses
  S3, where forks are serialized to an S3 prefix instead of a local folder.
- **Recommended Processor-interface direction (for the real build):** <fill in based on
  what felt awkward — e.g., a method to initialize full shape, and a per-worker method
  that maps a file/index to (chunk_key, location, offset, length) for set_virtual_ref>.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md
git commit -m "spike: record fork/merge backfill findings"
```

---

## Notes for the implementer

- All API usage here (`local_filesystem_storage`, `create_array` with `serializer`,
  `set_virtual_ref`, `Session.fork`/`merge`, `reset_branch`, spawn worker round-trip) was
  run successfully against icechunk 1.1.14 / zarr 3.1.5 while authoring the spec. If a step
  fails, that divergence is itself a finding — record it in Task 7 rather than papering over it.
- `multiprocessing` start method must be `spawn` (the default on macOS) to force genuine
  pickling — do not switch to `fork`, which would let a worker inherit live objects and
  mask serialization bugs.
- This is disposable spike code under `tests/spike/`. Do not wire it into `cdk/` or
  `lambda/`. The Step Functions / Distributed Map / Lambda / partitioner build is a separate,
  later effort informed by the findings note.
```
