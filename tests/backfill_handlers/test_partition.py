import boto3
from backfill_handlers import inventory, partition


def test_partition_splits_inventory_into_manifests(s3_bucket: str) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="inv.json", Body=b'["0", "1", "2", "3", "4"]'
    )
    event = {
        "inventory_uri": f"s3://{s3_bucket}/inv.json",
        "run_prefix": f"s3://{s3_bucket}/run/",
        "partition_size": 2,
    }

    result = partition.handler(event, None)

    parts = result["partitions"]
    assert [p["partition_id"] for p in parts] == ["0", "1", "2"]
    assert inventory.read_manifest(parts[0]["manifest_uri"]) == ["0", "1"]
    assert inventory.read_manifest(parts[2]["manifest_uri"]) == ["4"]
