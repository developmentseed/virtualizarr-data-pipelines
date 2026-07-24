from settings import StackSettings


def test_backfill_settings_defaults() -> None:
    settings = StackSettings(STAGE="dev", ACCOUNT_ID="111111111111")
    assert settings.BACKFILL_ENABLED is False
    assert settings.BACKFILL_PARTITION_SIZE == 500
    assert settings.BACKFILL_MAX_ITEMS_PER_BATCH == 10
    assert settings.BACKFILL_MAX_CONCURRENCY == 50


def test_forward_queue_enabled_defaults_on_when_backfill_off() -> None:
    settings = StackSettings(STAGE="dev", ACCOUNT_ID="111111111111")
    assert settings.FORWARD_QUEUE_ENABLED is True


def test_forward_queue_enabled_defaults_off_when_backfill_on() -> None:
    settings = StackSettings(
        STAGE="dev", ACCOUNT_ID="111111111111", BACKFILL_ENABLED=True
    )
    assert settings.FORWARD_QUEUE_ENABLED is False


def test_forward_queue_enabled_explicit_value_is_honored() -> None:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        BACKFILL_ENABLED=True,
        FORWARD_QUEUE_ENABLED=True,
    )
    assert settings.FORWARD_QUEUE_ENABLED is True


def test_forward_queue_disabled_explicit_with_backfill_off() -> None:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        FORWARD_QUEUE_ENABLED=False,
    )
    assert settings.FORWARD_QUEUE_ENABLED is False
