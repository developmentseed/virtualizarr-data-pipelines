import pathlib
from unittest.mock import MagicMock

import pytest
from backfill_handlers import init, promote
from virtualizarr_processor.processor import Processor


def test_promote_moves_main_to_backfill_tip(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    lambda_context: MagicMock,
) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    init.handler({}, lambda_context)

    result = promote.handler({}, lambda_context)

    assert result["promoted"] is True
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == repo.lookup_branch("backfill")
