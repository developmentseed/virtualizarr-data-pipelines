# Backfill Processor Interface (sub-project A) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a backfill data-model interface to `VirtualizarrProcessor` — three user-implemented Protocol methods (`initialize_backfill_store`, `region_for`, `process_backfill_file`), generic framework helpers (`create_fork`, `merge_and_commit`, `promote`), and a synthetic reference implementation — all TDD'd locally with no AWS.

**Architecture:** The backfill model initializes an Icechunk store at full shape on a `backfill` branch and commits (a clean base snapshot). A coordinator creates one fork; workers each `fork()` a child, write disjoint virtual chunk references via `set_virtual_ref`, and pickle the child back; a fresh writable session merges all children and commits once; promotion fast-forwards `main`. This sub-project builds the Processor-level pieces of that model. The existing append-based methods are untouched — backfill is additive.

**Tech Stack:** Python 3.11+, `uv`, `pytest`, `icechunk` 1.1.14, `zarr` 3.1.5, `obstore`, `numpy`. Every code block below was harvested from a prototype run against these exact versions.

**Spec:** `docs/superpowers/specs/2026-06-25-backfill-processor-interface-design.md`

---

## Critical implementation note (verified while planning)

Backfill tests MUST use `icechunk.local_filesystem_storage(tmp_path)`, **not** the existing
conftest's `in_memory_storage()`. A pickled `ForkSession` cannot resolve its base snapshot from
in-memory storage even in the same process — it fails with
`object store error Object at location snapshots/... not found: No data in memory found`, because
the in-memory backing is not carried across the pickle. Filesystem storage is durable and shared,
so the reloaded fork resolves. The append-path `initialize_repo()` keeps `in_memory_storage`; the
backfill path gets its own filesystem-backed fixture. `initialize_backfill_store(repo)` takes the
repo as a parameter and never dictates storage.

## File Structure

- **Modify `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`** — add three method
  signatures + docstrings to the `VirtualizarrProcessor` Protocol; add `ForkSession` and `Mapping`
  imports. One responsibility: the interface contract users implement.
- **Create `lambda/virtualizarr-processor/virtualizarr_processor/backfill.py`** — generic
  fork/merge/promote helpers. One responsibility: the icechunk operations that are identical for
  every user (not part of the Protocol).
- **Modify `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`** — add backfill
  constants and the synthetic reference implementation of the three Protocol methods.
- **Modify `tests/conftest.py`** — add a `backfill_repo` fixture (filesystem-backed repo with a
  committed `main` branch).
- **Create `tests/test_backfill.py`** — tests for the interface, helpers, and the full round-trip.

No production `cdk/` or `lambda/*/handler.py` code is touched in this sub-project.

---

### Task 1: Filesystem-backed `backfill_repo` fixture

The backfill round-trip needs a durable, shared repo (see the critical note). This fixture creates
one with an initialized `main` branch, ready for `initialize_backfill_store` to branch off.

**Files:**
- Modify: `tests/conftest.py`
- Test: `tests/test_backfill.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_backfill.py`:

```python
import icechunk


def test_backfill_repo_has_main_branch(backfill_repo: icechunk.Repository) -> None:
    assert "main" in backfill_repo.list_branches()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_backfill.py::test_backfill_repo_has_main_branch -v`
Expected: FAIL with `fixture 'backfill_repo' not found`.

- [ ] **Step 3: Add the fixture to `tests/conftest.py`**

`tests/conftest.py` already imports `icechunk` but does NOT import top-level `zarr` (verified — it
only has `from zarr.codecs import BytesCodec`). Add `import zarr` to the third-party import block
(next to `import obstore`), keep all existing content, then append the fixture:

```python
@pytest.fixture(scope="function")
def backfill_repo(tmp_path) -> icechunk.Repository:
    """A filesystem-backed repo with a committed `main` branch.

    Backfill uses durable storage (not in_memory_storage) because a pickled
    ForkSession cannot resolve its base snapshot from an in-memory backing.
    """
    chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
    storage = icechunk.local_filesystem_storage(str(tmp_path / "repo"))
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
    )
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access={CHUNK_DIRECTORY_URL_PREFIX: None},
    )
    session = repo.writable_session("main")
    zarr.open_group(session.store, mode="a").create_group("placeholder")
    session.commit("init main")
    return repo
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_backfill.py::test_backfill_repo_has_main_branch -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/conftest.py tests/test_backfill.py
git commit -m "test: filesystem-backed backfill_repo fixture"
```

---

### Task 2: Protocol additions + synthetic reference methods

Add the three backfill methods to the Protocol AND their synthetic implementations on `Processor`
in the same task, so the existing `test_follows_protocol` (runtime `isinstance` check) stays green.

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`
- Test: `tests/test_backfill.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_backfill.py`:

```python
import zarr
from virtualizarr_processor.processor import Processor


def test_region_for_is_deterministic() -> None:
    processor = Processor()
    assert processor.region_for("3") == {"time": 3}
    assert processor.region_for("3") == processor.region_for("3")


def test_initialize_backfill_store_creates_full_shape(
    backfill_repo: icechunk.Repository,
) -> None:
    processor = Processor()
    snapshot = processor.initialize_backfill_store(backfill_repo)

    assert isinstance(snapshot, str) and snapshot
    assert "backfill" in backfill_repo.list_branches()
    session = backfill_repo.readonly_session("backfill")
    arr = zarr.open_group(session.store, mode="r")["foo"]
    assert arr.shape == (6, 2, 3)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_backfill.py -k "region_for or full_shape" -v`
Expected: FAIL with `AttributeError: 'Processor' object has no attribute 'region_for'`.

- [ ] **Step 3a: Add the Protocol methods to `typing.py`**

At the top of `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`, add these imports
next to the existing ones (`from icechunk import Repository, Session` is already there):

```python
from collections.abc import Mapping

from icechunk import ForkSession, Repository, Session
```

Then, inside the `VirtualizarrProcessor` Protocol class, add these three methods (after the
existing `commit_processed_files` method, before `garbage_collect`):

```python
    def initialize_backfill_store(self, repo: Repository) -> str:
        """
        Create the `backfill` branch off the current `main` tip and build the
        full-shape array(s) and coordinates (metadata only), commit, and return
        the base snapshot id.

        The store is declared at its full extent up front because backfill writes
        disjoint regions via set_virtual_ref rather than appending. The session
        MUST have no uncommitted changes after this returns, so that forks taken
        from a fresh session share the committed branch-tip snapshot as their base.

        Parameters
        ----------
            repo: An Icechunk Repository (durable storage; not in-memory).
        Returns
        -------
        str
            The base snapshot id of the committed full-shape store.
        """
        ...

    def region_for(self, file_key: str) -> Mapping[str, int]:
        """
        Map a file key to its absolute index/region in the pre-sized array.

        Must be deterministic and side-effect-free so the partitioner can call it
        to assign and verify disjoint partitions.

        Parameters
        ----------
            file_key: The full key path to the source file.
        Returns
        -------
        Mapping[str, int]
            A per-dimension index map, e.g. {"time": 42}.
        """
        ...

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        """
        Write the file's virtual references into the fork's store at
        region_for(file_key) via set_virtual_ref. Must NOT commit.

        Parameters
        ----------
            file_key: The full key path to the source file.
            fork: An Icechunk ForkSession to write references into.
        Returns
        -------
        bool
            True if the file was successfully processed.
        """
        ...
```

- [ ] **Step 3b: Add constants and reference methods to `processor.py`**

In `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`, the reference methods use
`zarr.open_group`, but the file currently only imports `from zarr.codecs import BytesCodec` — it
does NOT import top-level `zarr` (verified). Add `import zarr` to the third-party import block
(next to `import obstore`). Then add these imports next to the existing ones and the constants
after the existing `CHUNK_DIRECTORY_URL_PREFIX` definition:

```python
import zarr  # add to the existing third-party import block

from collections.abc import Mapping
from typing import cast

from icechunk import ForkSession, Repository  # Repository is already imported; add ForkSession

# Backfill synthetic dataset: N time steps, each a (Y, X) int32 chunk.
BACKFILL_N, BACKFILL_Y, BACKFILL_X = 6, 2, 3
BACKFILL_DTYPE = np.dtype("int32")
BACKFILL_CHUNK_NBYTES = BACKFILL_Y * BACKFILL_X * BACKFILL_DTYPE.itemsize
BACKFILL_SOURCE_PATH = f"{CHUNK_DIR}/backfill_source.bin"
BACKFILL_SOURCE_URL = f"file://{BACKFILL_SOURCE_PATH}"
```

Then add these three methods to the `Processor` class (alongside the existing methods):

```python
    def initialize_backfill_store(self, repo: Repository) -> str:
        # Write the synthetic source: N back-to-back chunks, chunk t filled with
        # value t, so chunk t is at byte offset t * BACKFILL_CHUNK_NBYTES.
        buf = b"".join(
            np.full((BACKFILL_Y, BACKFILL_X), t, dtype=BACKFILL_DTYPE).tobytes()
            for t in range(BACKFILL_N)
        )
        obstore.put(obstore.store.LocalStore(), BACKFILL_SOURCE_PATH, buf)

        repo.create_branch("backfill", repo.lookup_branch("main"))
        session = repo.writable_session("backfill")
        root = zarr.open_group(session.store, mode="a")
        root.create_array(
            "foo",
            shape=(BACKFILL_N, BACKFILL_Y, BACKFILL_X),
            chunks=(1, BACKFILL_Y, BACKFILL_X),
            dtype=BACKFILL_DTYPE,
            serializer=BytesCodec(),
            compressors=None,
            filters=None,
            dimension_names=("time", "y", "x"),
        )
        return cast(str, session.commit("Initialize backfill shape"))

    def region_for(self, file_key: str) -> Mapping[str, int]:
        # Synthetic keys are the integer time index as a string ("0".."5").
        # Real implementations would parse their own scheme (e.g. a date).
        return {"time": int(file_key)}

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        try:
            t = self.region_for(file_key)["time"]
            fork.store.set_virtual_ref(
                f"foo/c/{t}/0/0",
                BACKFILL_SOURCE_URL,
                offset=t * BACKFILL_CHUNK_NBYTES,
                length=BACKFILL_CHUNK_NBYTES,
                validate_container=False,
            )
            return True
        except Exception:
            return False
```

`zarr`, `obstore`, `np`, `BytesCodec`, and `Repository` are already imported in `processor.py`.
Add any that are missing (verify against the file's existing import block).

- [ ] **Step 4: Run tests to verify they pass (and conformance stays green)**

Run: `uv run pytest tests/test_backfill.py -k "region_for or full_shape" tests/test_example.py::test_follows_protocol -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/virtualizarr-processor/virtualizarr_processor/typing.py lambda/virtualizarr-processor/virtualizarr_processor/processor.py tests/test_backfill.py
git commit -m "feat: backfill Protocol methods + synthetic reference impl"
```

---

### Task 3: Framework helpers + full round-trip

Add the generic `create_fork` / `merge_and_commit` / `promote` helpers and a full round-trip test
that exercises them together with `process_backfill_file`.

**Files:**
- Create: `lambda/virtualizarr-processor/virtualizarr_processor/backfill.py`
- Test: `tests/test_backfill.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_backfill.py`:

```python
import numpy as np
from virtualizarr_processor import backfill


def _worker(shared_fork_bytes: bytes, keys: list[str]) -> bytes:
    import pickle

    processor = Processor()
    child = pickle.loads(shared_fork_bytes).fork()
    for key in keys:
        assert processor.process_backfill_file(key, child)
    return pickle.dumps(child)


def test_full_backfill_round_trip(backfill_repo: icechunk.Repository) -> None:
    processor = Processor()
    processor.initialize_backfill_store(backfill_repo)

    shared = backfill.create_fork(backfill_repo)
    child_a = _worker(shared, ["0", "1", "2"])
    child_b = _worker(shared, ["3", "4", "5"])

    tip = backfill.merge_and_commit(
        backfill_repo, [child_a, child_b], message="backfill commit"
    )
    assert isinstance(tip, str) and tip

    arr = zarr.open_group(
        backfill_repo.readonly_session("backfill").store, mode="r"
    )["foo"]
    expected = np.arange(6)[:, None, None]
    assert (np.asarray(arr[:]) == expected).all()

    backfill.promote(backfill_repo)
    arr_main = zarr.open_group(
        backfill_repo.readonly_session("main").store, mode="r"
    )["foo"]
    assert (np.asarray(arr_main[:]) == expected).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_backfill.py::test_full_backfill_round_trip -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'virtualizarr_processor.backfill'`.

- [ ] **Step 3: Create `lambda/virtualizarr-processor/virtualizarr_processor/backfill.py`**

```python
"""Generic Icechunk fork/merge/promote helpers for backfill.

These operations are identical for every VirtualizarrProcessor implementation,
so they live here rather than on the Protocol. All were verified against
icechunk 1.1.14: a fresh writable session can merge forks created by an earlier
(now-discarded) session and commit, as long as the fork's base is a committed
branch-tip snapshot.
"""

import pickle
from typing import cast

from icechunk import Repository


def create_fork(repo: Repository, branch: str = "backfill") -> bytes:
    """Open a fresh writable session on `branch` and return a pickled fork.

    The session is clean (init already committed), so the fork's base is the
    committed branch-tip snapshot. The single returned artifact is distributed
    to all workers, which each call fork() to make their own child.
    """
    session = repo.writable_session(branch)
    return pickle.dumps(session.fork())


def merge_and_commit(
    repo: Repository,
    child_fork_bytes: list[bytes],
    *,
    branch: str = "backfill",
    message: str,
) -> str:
    """Open a fresh writable session, merge all child forks, and commit once.

    Returns the new tip snapshot id.
    """
    session = repo.writable_session(branch)
    forks = [pickle.loads(b) for b in child_fork_bytes]
    session.merge(*forks)
    # cast: pre-commit mypy runs without icechunk, so commit() is Any there and
    # warn_return_any flags a bare return. Do not remove.
    return cast(str, session.commit(message))


def promote(repo: Repository, *, source: str = "backfill", target: str = "main") -> None:
    """Fast-forward `target` to the current tip of `source`."""
    repo.reset_branch(target, repo.lookup_branch(source))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_backfill.py::test_full_backfill_round_trip -v`
Expected: PASS (all 6 slices correct on both `backfill` and `main`).

- [ ] **Step 5: Run the whole suite to confirm no regressions**

Run: `uv run pytest -q`
Expected: all tests pass (existing append tests + new backfill tests).

- [ ] **Step 6: Commit**

```bash
git add lambda/virtualizarr-processor/virtualizarr_processor/backfill.py tests/test_backfill.py
git commit -m "feat: backfill fork/merge/promote helpers + round-trip test"
```

---

## Notes for the implementer

- Every code block was harvested from a working prototype against icechunk 1.1.14 / zarr 3.1.5.
  If a step diverges, that is itself a finding — report it rather than papering over it.
- Do NOT change the existing `initialize_repo()` (it uses `in_memory_storage` for the append
  path). Backfill deliberately uses filesystem storage via the `backfill_repo` fixture.
- Do NOT add region-overlap detection anywhere — the spike proved `merge` is last-writer-wins;
  disjointness is guaranteed by the partitioner in sub-project B, not here.
- The pre-commit hooks (ruff 88-char + imports-at-top, ruff-format, mypy) will run on commit.
  mypy runs in an isolated env WITHOUT icechunk, so any function returning an icechunk call typed
  `-> str` needs `typing.cast` (already applied in `initialize_backfill_store` and
  `merge_and_commit`). Apply hook-required fixes, re-add, and commit.
- This is sub-project A. Handlers, partitioner, and CDK are sub-projects B and C — do not build
  them here.
```
