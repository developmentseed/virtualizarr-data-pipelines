from collections.abc import Iterator

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
