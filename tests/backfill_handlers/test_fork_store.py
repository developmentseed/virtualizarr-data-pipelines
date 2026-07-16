from backfill_handlers import fork_store


def test_save_then_load_round_trips(s3_bucket: str) -> None:
    uri = f"s3://{s3_bucket}/forks/0/in/fork.pkl"
    fork_store.save_fork(uri, b"\x01\x02\x03")
    assert fork_store.load_fork(uri) == b"\x01\x02\x03"


def test_list_forks_returns_all_uris_under_prefix(s3_bucket: str) -> None:
    prefix = f"s3://{s3_bucket}/forks/0/out/"
    fork_store.save_fork(prefix + "a.pkl", b"a")
    fork_store.save_fork(prefix + "b.pkl", b"b")
    listed = fork_store.list_forks(prefix)
    assert sorted(listed) == [prefix + "a.pkl", prefix + "b.pkl"]


def test_list_forks_returns_empty_list_for_unknown_prefix(s3_bucket: str) -> None:
    assert fork_store.list_forks(f"s3://{s3_bucket}/forks/nonexistent/") == []
