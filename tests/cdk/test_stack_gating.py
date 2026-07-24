import aws_cdk as cdk
from aws_cdk.assertions import Template
from settings import StackSettings
from stack import VirtualizarrSqsStack


def _template(*, backfill: bool, forward: bool | None = None) -> Template:
    kwargs = dict(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        DATA_BUCKET_NAME="data-test",
        BACKFILL_ENABLED=backfill,
    )
    if forward is not None:
        kwargs["FORWARD_QUEUE_ENABLED"] = forward
    settings = StackSettings(**kwargs)
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


def _synth(enabled: bool) -> Template:
    return _template(backfill=enabled)


def test_backfill_disabled_creates_no_state_machine() -> None:
    _synth(False).resource_count_is("AWS::StepFunctions::StateMachine", 0)


def test_backfill_enabled_creates_state_machine() -> None:
    _synth(True).resource_count_is("AWS::StepFunctions::StateMachine", 1)


def test_forward_queue_enabled_when_backfill_off() -> None:
    _template(backfill=False).has_resource_properties(
        "AWS::Lambda::EventSourceMapping", {"Enabled": True}
    )


def test_forward_queue_disabled_when_backfill_on() -> None:
    _template(backfill=True).has_resource_properties(
        "AWS::Lambda::EventSourceMapping", {"Enabled": False}
    )


def test_forward_queue_explicit_enable_with_backfill_on() -> None:
    _template(backfill=True, forward=True).has_resource_properties(
        "AWS::Lambda::EventSourceMapping", {"Enabled": True}
    )
