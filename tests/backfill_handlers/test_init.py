import pathlib
from unittest.mock import MagicMock

import pytest
from backfill_handlers import init


def test_init_creates_backfill_branch(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    lambda_context: MagicMock,
) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))

    result = init.handler({}, lambda_context)

    assert isinstance(result["base_snapshot"], str) and result["base_snapshot"]
