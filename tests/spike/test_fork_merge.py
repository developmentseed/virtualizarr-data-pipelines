import os
import pathlib
import sys

# tests/spike is on sys.path under pytest's default prepend import mode (no
# __init__.py), so backfill_spike imports as a top-level module — required for
# multiprocessing spawn.
sys.path.insert(0, os.path.dirname(__file__))

import backfill_spike as bk


def test_open_repo_creates_main_branch(tmp_path: pathlib.Path) -> None:
    repo = bk.open_repo(str(tmp_path))
    assert "main" in repo.list_branches()
