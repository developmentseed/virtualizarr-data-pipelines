"""Shared S3 plumbing for the backfill handlers."""

from typing import Any


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri}")
    bucket, _, key = uri[len("s3://") :].partition("/")
    return bucket, key


def s3_client() -> Any:
    """Return a boto3 S3 client (region from the AWS environment)."""
    import boto3

    return boto3.client("s3")
