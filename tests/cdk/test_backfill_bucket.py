"""Backfill artifacts live in their own in-region bucket.

StepFunctions' S3JsonItemReader takes no region and assumes the stack's own, so
partition manifests cannot be read out of an Icechunk bucket in another region.
The artifacts bucket is created by this stack, and so is always in-region.
"""

import json
from typing import Any

import aws_cdk as cdk
from aws_cdk.assertions import Template
from settings import StackSettings
from stack import VirtualizarrSqsStack
from stack_constructs.backfill_pipeline import _ACTIONS

ICECHUNK_BUCKET = "ice-existing"
BACKFILL_BUCKET = "backfill-existing"


def _template(**overrides: Any) -> Template:
    """Both buckets adopted by name, so the synthesized template carries the
    names as literals rather than as refs."""
    settings = StackSettings(
        **{
            "STAGE": "dev",
            "ACCOUNT_ID": "111111111111",
            "DATA_BUCKET_NAME": "data-test",
            "BACKFILL_ENABLED": True,
            "ICECHUNK_BUCKET": ICECHUNK_BUCKET,
            "BACKFILL_BUCKET": BACKFILL_BUCKET,
        }
        | overrides
    )
    app = cdk.App()
    stack = VirtualizarrSqsStack(
        app,
        settings.STACK_NAME,
        settings=settings,
        env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
    )
    return Template.from_stack(stack)


def _definition(template: Template) -> str:
    (machine,) = template.find_resources("AWS::StepFunctions::StateMachine").values()
    return json.dumps(machine["Properties"]["DefinitionString"])


def _bucket_names(template: Template) -> list[str]:
    return [
        resource["Properties"].get("BucketName")
        for resource in template.find_resources("AWS::S3::Bucket").values()
    ]


def test_state_machine_addresses_only_the_backfill_bucket() -> None:
    """The item reader's bucket and the run prefix the manifests and forks are
    written under both have to be reachable from the stack's region."""
    definition = _definition(_template())

    assert BACKFILL_BUCKET in definition
    assert ICECHUNK_BUCKET not in definition


def test_every_handler_can_read_and_write_the_backfill_bucket() -> None:
    policies = [
        json.dumps(resource)
        for resource in _template().find_resources("AWS::IAM::Policy").values()
    ]

    granting = [policy for policy in policies if BACKFILL_BUCKET in policy]
    # one role per handler, plus the state machine's own read for the item reader
    assert len(granting) >= len(_ACTIONS)


def test_bucket_is_created_when_no_existing_one_is_named() -> None:
    template = _template(BACKFILL_BUCKET=None, BACKFILL_BUCKET_NAME="made-here")

    assert "made-here" in _bucket_names(template)


def test_no_bucket_without_backfill() -> None:
    """A forward-only deployment has no artifacts to keep, so it gets no bucket."""
    template = _template(
        BACKFILL_ENABLED=False, BACKFILL_BUCKET=None, BACKFILL_BUCKET_NAME="made-here"
    )

    assert "made-here" not in _bucket_names(template)
