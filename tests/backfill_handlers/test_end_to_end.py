import pathlib
from unittest.mock import MagicMock

import boto3
import numpy as np
import pytest
import zarr
from backfill_handlers import fork, init, inventory, partition, promote, reduce, worker
from virtualizarr_processor.processor import Processor


def test_full_backfill_chain(
    s3_bucket: str,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    lambda_context: MagicMock,
) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    run_prefix = f"s3://{s3_bucket}/run/"

    # Inventory: 6 synthetic keys "0".."5" (one per time slice).
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="inv.json", Body=b'["0", "1", "2", "3", "4", "5"]'
    )

    # partition -> init
    parts = partition.handler(
        {
            "inventory_uri": f"s3://{s3_bucket}/inv.json",
            "run_prefix": run_prefix,
            "partition_size": 3,
        },
        lambda_context,
    )["partitions"]
    init.handler({}, lambda_context)

    # serial over partitions: fork -> workers (one per file) -> reduce
    for part in parts:
        fork_result = fork.handler(
            {
                "partition_id": part["partition_id"],
                "manifest_uri": part["manifest_uri"],
                "run_prefix": run_prefix,
            },
            lambda_context,
        )
        for file_key in inventory.read_manifest(part["manifest_uri"]):
            worker.handler(
                {
                    "fork_in_uri": fork_result["fork_in_uri"],
                    "forks_out_prefix": fork_result["forks_out_prefix"],
                    "file_keys": [file_key],
                },
                lambda_context,
            )
        reduce.handler(
            {
                "partition_id": part["partition_id"],
                "forks_out_prefix": fork_result["forks_out_prefix"],
            },
            lambda_context,
        )

    # promote and verify all 6 slices on main
    promote.handler({}, lambda_context)
    repo = Processor().open_backfill_repo()
    arr = zarr.open_group(repo.readonly_session("main").store, mode="r")["foo"]
    assert (np.asarray(arr[:]) == np.arange(6)[:, None, None]).all()
