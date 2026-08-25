"""The Icechunk bucket's region is configured, not inherited from the deploy."""

from typing import Any

import aws_cdk as cdk
from aws_cdk.assertions import Template
from settings import StackSettings
from stack import VirtualizarrSqsStack


def _icechunk_environments(**overrides: Any) -> list[dict[str, Any]]:
    """The environment of every Lambda that opens the Icechunk store."""
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        ICECHUNK_BUCKET_NAME="ice-test",
        DATA_BUCKET_NAME="data-test",
        BACKFILL_ENABLED=True,
        **overrides,
    )
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    template = Template.from_stack(stack)
    environments = [
        resource["Properties"].get("Environment", {}).get("Variables", {})
        for resource in template.find_resources("AWS::Lambda::Function").values()
    ]
    return [env for env in environments if "ICECHUNK_BUCKET" in env]


def test_region_is_left_to_the_runtime_when_not_configured() -> None:
    """Without an explicit region icechunk resolves the bucket's region itself,
    rather than being pinned to whichever region the stack was deployed into."""
    environments = _icechunk_environments()

    assert environments
    for environment in environments:
        assert "ICECHUNK_REGION" not in environment


def test_configured_region_reaches_every_icechunk_lambda() -> None:
    """A bucket in a different region than the stack needs its region stated,
    and forward and backfill handlers open the same store."""
    environments = _icechunk_environments(ICECHUNK_REGION="us-west-2")

    assert environments
    for environment in environments:
        assert environment["ICECHUNK_REGION"] == "us-west-2"
