import pathlib
from unittest.mock import MagicMock

import pytest
from backfill_handlers import fork, fork_store, init


def test_fork_writes_shared_fork_blob(
    s3_bucket: str,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    lambda_context: MagicMock,
) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    init.handler({}, lambda_context)

    event = {
        "partition_id": "0",
        "manifest_uri": f"s3://{s3_bucket}/run/partitions/0.json",
        "run_prefix": f"s3://{s3_bucket}/run/",
    }
    result = fork.handler(event, lambda_context)

    assert result["fork_in_uri"] == f"s3://{s3_bucket}/run/forks/0/in/fork.pkl"
    assert result["forks_out_prefix"] == f"s3://{s3_bucket}/run/forks/0/out/"
    assert result["partition_id"] == "0"
    assert result["manifest_uri"] == event["manifest_uri"]
    assert len(fork_store.load_fork(result["fork_in_uri"])) > 0
