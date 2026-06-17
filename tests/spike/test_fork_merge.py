import os
import pathlib
import pickle
import sys

import numpy as np
import zarr

# tests/spike is on sys.path under pytest's default prepend import mode (no
# __init__.py), so backfill_spike imports as a top-level module — required for
# multiprocessing spawn.
sys.path.insert(0, os.path.dirname(__file__))

import backfill_spike as bk


def test_open_repo_creates_main_branch(tmp_path: pathlib.Path) -> None:
    repo = bk.open_repo(str(tmp_path))
    assert "main" in repo.list_branches()


def test_init_creates_backfill_branch_and_full_shape_array(
    tmp_path: pathlib.Path,
) -> None:
    work = str(tmp_path)
    repo = bk.open_repo(work)
    bk.write_source(work)
    bk.init_backfill_store(repo, work)

    assert "backfill" in repo.list_branches()
    session = repo.readonly_session("backfill")
    arr = zarr.open_group(session.store, mode="r")["foo"]
    assert arr.shape == (bk.N, bk.Y, bk.X)
    assert arr.dtype == bk.DTYPE


def test_worker_writes_refs_in_process(tmp_path: pathlib.Path) -> None:
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


def test_cross_process_fork_merge_commits_all_slices(tmp_path: pathlib.Path) -> None:
    work = str(tmp_path)
    repo = bk.open_repo(work)
    bk.write_source(work)
    bk.init_backfill_store(repo, work)

    tip = bk.run_backfill(repo, work, subsets=[[0, 1, 2], [3, 4, 5]])
    assert tip  # non-empty snapshot id

    arr = zarr.open_group(repo.readonly_session("backfill").store, mode="r")["foo"]
    for t in range(bk.N):
        assert (np.asarray(arr[t]) == t).all(), (t, np.asarray(arr[t]))
