"""Read the S3 inventory file and read/write partition manifests (JSON key lists)."""

import json
from typing import cast

from backfill_handlers.config import parse_s3_uri, s3_client


def read_inventory(uri: str) -> list[str]:
    """Read a JSON array of file keys from the inventory object."""
    bucket, key = parse_s3_uri(uri)
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    # cast: json.loads returns Any; the isolated mypy env would flag the return.
    return cast(list[str], json.loads(body))


def write_manifest(uri: str, keys: list[str]) -> None:
    """Write a partition manifest (JSON array of keys) to S3."""
    bucket, key = parse_s3_uri(uri)
    s3_client().put_object(Bucket=bucket, Key=key, Body=json.dumps(keys).encode())


def read_manifest(uri: str) -> list[str]:
    """Read a partition manifest (JSON array of keys) from S3."""
    bucket, key = parse_s3_uri(uri)
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    return cast(list[str], json.loads(body))
