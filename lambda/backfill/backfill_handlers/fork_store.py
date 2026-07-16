"""Save, load, and list pickled fork blobs in S3."""

from typing import cast

from backfill_handlers.config import parse_s3_uri, s3_client


def save_fork(uri: str, data: bytes) -> None:
    """Write a pickled fork blob to S3."""
    bucket, key = parse_s3_uri(uri)
    s3_client().put_object(Bucket=bucket, Key=key, Body=data)


def load_fork(uri: str) -> bytes:
    """Read a pickled fork blob from S3."""
    bucket, key = parse_s3_uri(uri)
    return cast(bytes, s3_client().get_object(Bucket=bucket, Key=key)["Body"].read())


def list_forks(prefix: str) -> list[str]:
    """List all object URIs under an ``s3://`` prefix."""
    bucket, key_prefix = parse_s3_uri(prefix)
    paginator = s3_client().get_paginator("list_objects_v2")
    uris: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            uris.append(f"s3://{bucket}/{obj['Key']}")
    return uris
