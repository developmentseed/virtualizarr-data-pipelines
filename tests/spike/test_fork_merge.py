import os
import pathlib
import sys

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
