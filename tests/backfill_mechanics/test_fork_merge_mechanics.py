"""Regression tests for the two low-level Icechunk backfill mechanics that the
production code (sub-projects A and B) assumes but does not itself exercise:

1. cross-process fork/merge over a REAL process boundary (multiprocessing spawn,
   genuine pickling) — A uses in-process pickle and B uses in-process handler
   calls, so this is the only place the spawn/serialisation path is proven;
2. merge does NOT detect overlapping-chunk writes (last-writer-wins) — the
   behaviour that makes disjointness the operator's responsibility.

These graduated out of the disposable spike (tests/spike/, removed); the other
spike tests are now covered by tests/test_backfill.py and
tests/backfill_handlers/. The harness lives in mechanics_harness.py and must be
importable as a top-level module (no __init__.py here) so a spawned worker
process can re-import its `run_worker` target.
"""

import os
import pathlib
import sys

import numpy as np
import zarr

sys.path.insert(0, os.path.dirname(__file__))

import mechanics_harness as mh


def test_cross_process_fork_merge_commits_all_slices(tmp_path: pathlib.Path) -> None:
    work = str(tmp_path)
    repo = mh.open_repo(work)
    mh.init_backfill_store(repo, work)

    tip = mh.run_backfill(repo, work, subsets=[[0, 1, 2], [3, 4, 5]])
    assert tip  # non-empty snapshot id

    arr = zarr.open_group(repo.readonly_session("backfill").store, mode="r")["foo"]
    for t in range(mh.N):
        assert (np.asarray(arr[t]) == t).all(), (t, np.asarray(arr[t]))


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
    # same index 0, different value 5
    mh._slice_vds_value(work, index=0, value=5).vz.to_icechunk(
        fork_b.store, region="auto", validate_containers=False
    )

    session.merge(fork_a, fork_b)
    session.commit("overlapping region writes")

    arr = zarr.open_group(repo.readonly_session("backfill").store, mode="r")["foo"]
    assert (np.asarray(arr[0]) == 5).all()
