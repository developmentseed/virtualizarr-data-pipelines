import icechunk


def test_backfill_repo_has_main_branch(backfill_repo: icechunk.Repository) -> None:
    assert "main" in backfill_repo.list_branches()
