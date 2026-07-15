import icechunk
import zarr
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
