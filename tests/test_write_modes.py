"""Write-mode selection for forward processing: create, region or append."""

import icechunk
import numpy as np
import xarray as xr
from virtualizarr_processor.processor import (
    APPEND_DIMENSION,
    Processor,
    store_append_dimension,
    write_plan,
)


def plain_dataset(time: str, dimension: str = APPEND_DIMENSION) -> xr.Dataset:
    """A cycle's shape without the virtual chunks: same dims and coordinate as
    `synthetic_vds`, so the mode rules can be checked without a store."""
    return xr.Dataset(
        {"foo": ((dimension, "y", "x"), np.zeros((1, 2, 3), dtype="int32"))},
        coords={
            dimension: (dimension, np.array([time], dtype="datetime64[ns]")),
            "y": ("y", np.arange(2)),
            "x": ("x", np.arange(3)),
        },
    )


def test_write_plan_creates_when_the_store_holds_nothing() -> None:
    """The first file of a forward-only deployment has no array to write into,
    so the write has to create one."""
    plan = write_plan(plain_dataset("2024-01-01"), None)

    assert plan.mode == "create"
    assert plan.kwargs == {}


def test_write_plan_appends_a_coordinate_off_the_end_of_the_axis() -> None:
    existing = np.array(["2024-01-01"], dtype="datetime64[ns]")

    plan = write_plan(plain_dataset("2024-01-02"), existing)

    assert plan.mode == "append"
    assert plan.kwargs == {"append_dim": APPEND_DIMENSION}


def test_write_plan_regions_a_coordinate_already_on_the_axis() -> None:
    """Appending a coordinate the store already carries would store the file
    twice and leave the axis non-monotonic, so it is written in place."""
    existing = np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]")

    plan = write_plan(plain_dataset("2024-01-02"), existing)

    assert plan.mode == "region"
    assert plan.kwargs == {"region": "auto"}


def test_write_plan_matches_across_datetime_resolutions() -> None:
    """A store's axis decodes at whatever resolution its units imply, which need
    not be the dataset's nanoseconds; the file is still the same file."""
    existing = np.array(["2024-01-01"], dtype="datetime64[s]")

    assert write_plan(plain_dataset("2024-01-01"), existing).mode == "region"
    assert write_plan(plain_dataset("2024-01-02"), existing).mode == "append"


def test_store_append_dimension_is_none_when_the_store_holds_nothing(
    icechunk_repo: icechunk.Repository,
) -> None:
    """A forward-only deployment before its first file has no axis to read."""
    session = icechunk_repo.writable_session("main")

    assert store_append_dimension(session.store) is None


def test_store_append_dimension_reads_the_axis_the_store_holds(
    icechunk_session: icechunk.Session,
) -> None:
    existing = store_append_dimension(icechunk_session.store)

    assert existing is not None
    assert list(existing) == [np.datetime64("2024-01-01", "ns")]


def test_reprocessing_a_file_rewrites_its_row(
    icechunk_repo: icechunk.Repository,
) -> None:
    """The three modes in the order a deployment meets them: the first file
    creates the store, a re-delivery of it is written in place, and a new file
    extends the axis. The re-delivery must leave one row, not two."""
    processor = Processor()
    session = processor.initialize_session(icechunk_repo)

    assert processor.process_file("2024-01-01", session)  # create
    assert processor.process_file("2024-01-01", session)  # same file -> region
    assert processor.process_file("2024-01-02", session)  # new file -> append

    dataset = xr.open_zarr(session.store, consolidated=False, zarr_format=3)
    assert list(dataset[APPEND_DIMENSION].values) == [
        np.datetime64("2024-01-01", "ns"),
        np.datetime64("2024-01-02", "ns"),
    ]
    first, second = dataset["foo"].values
    # both rows resolve through their virtual chunks and neither is fill value,
    # so the region write put real references in place rather than leaving the
    # row it targeted empty
    assert first.any() and second.any()
    assert (first == second).all()


def test_write_plan_is_not_specific_to_a_time_axis() -> None:
    """The comparison is by coordinate value, so a store keyed on something
    other than time picks its mode the same way."""
    dataset = xr.Dataset(
        {"foo": (("step", "y"), np.zeros((1, 2), dtype="int32"))},
        coords={"step": ("step", np.array([7]))},
    )
    existing = np.array([5, 6, 7])

    assert write_plan(dataset, existing, dimension="step").mode == "region"
    assert write_plan(dataset, existing[:2], dimension="step").mode == "append"
