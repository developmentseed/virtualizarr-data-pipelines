import boto3
from backfill_handlers import inventory


def test_read_inventory_returns_keys(s3_bucket: str) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="inv.json", Body=b'["a", "b", "c"]'
    )
    assert inventory.read_inventory(f"s3://{s3_bucket}/inv.json") == ["a", "b", "c"]


def test_write_then_read_manifest_round_trips(s3_bucket: str) -> None:
    uri = f"s3://{s3_bucket}/partitions/0.json"
    inventory.write_manifest(uri, ["k1", "k2"])
    assert inventory.read_manifest(uri) == ["k1", "k2"]
