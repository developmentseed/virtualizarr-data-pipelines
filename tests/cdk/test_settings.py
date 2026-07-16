from settings import StackSettings


def test_backfill_settings_defaults() -> None:
    settings = StackSettings(STAGE="dev", ACCOUNT_ID="111111111111")
    assert settings.BACKFILL_ENABLED is False
    assert settings.BACKFILL_PARTITION_SIZE == 500
    assert settings.BACKFILL_MAX_ITEMS_PER_BATCH == 10
    assert settings.BACKFILL_MAX_CONCURRENCY == 50
