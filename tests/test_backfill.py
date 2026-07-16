import pathlib

import icechunk
import numpy as np
import pytest
import zarr
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor


def test_backfill_repo_has_main_branch(backfill_repo: icechunk.Repository) -> None:
    assert "main" in backfill_repo.list_branches()


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
    # chunk geometry + dtype are load-bearing for process_backfill_file's
    # chunk-key and byte-offset arithmetic; assert them here to catch a
    # geometry regression before the Task 3 round-trip.
    assert arr.chunks == (1, 2, 3)
    assert arr.dtype == np.dtype("int32")


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

    arr = zarr.open_group(backfill_repo.readonly_session("backfill").store, mode="r")[
        "foo"
    ]
    expected = np.arange(6)[:, None, None]
    assert (np.asarray(arr[:]) == expected).all()

    backfill.promote(backfill_repo)
    arr_main = zarr.open_group(backfill_repo.readonly_session("main").store, mode="r")[
        "foo"
    ]
    assert (np.asarray(arr_main[:]) == expected).all()


def test_open_backfill_repo_local_filesystem(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    processor = Processor()

    repo = processor.open_backfill_repo()

    assert isinstance(repo, icechunk.Repository)
    assert "main" in repo.list_branches()
    # main must have a resolvable tip so initialize_backfill_store can branch off it
    assert repo.lookup_branch("main")
