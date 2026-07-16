import aws_cdk as cdk
from aws_cdk.assertions import Template
from settings import StackSettings
from stack import VirtualizarrSqsStack


def _synth(enabled: bool) -> Template:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        DATA_BUCKET_NAME="data-test",
        BACKFILL_ENABLED=enabled,
    )
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


def test_backfill_disabled_creates_no_state_machine() -> None:
    _synth(False).resource_count_is("AWS::StepFunctions::StateMachine", 0)


def test_backfill_enabled_creates_state_machine() -> None:
    _synth(True).resource_count_is("AWS::StepFunctions::StateMachine", 1)
