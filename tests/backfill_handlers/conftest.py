from collections.abc import Iterator
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

BUCKET = "test-backfill-bucket"


@pytest.fixture()
def s3_bucket() -> Iterator[str]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield BUCKET


@pytest.fixture()
def lambda_context() -> MagicMock:
    """A stand-in Lambda context.

    powertools' @logger.inject_lambda_context reads context.function_name etc.
    at invocation, so handlers cannot be called with None; a MagicMock supplies
    any attribute (matching the existing tests/test_handler.py convention).
    """
    return MagicMock()
