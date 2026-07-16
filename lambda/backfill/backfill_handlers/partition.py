"""Handler: split the S3 inventory into partition manifests."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from backfill_handlers import inventory

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    keys = inventory.read_inventory(event["inventory_uri"])
    size = int(event["partition_size"])
    run_prefix = event["run_prefix"]

    partitions: list[dict[str, str]] = []
    for i in range(0, len(keys), size):
        partition_id = str(i // size)
        manifest_uri = f"{run_prefix}partitions/{partition_id}.json"
        inventory.write_manifest(manifest_uri, keys[i : i + size])
        partitions.append({"partition_id": partition_id, "manifest_uri": manifest_uri})

    logger.info("Partitioned inventory", extra={"count": len(partitions)})
    return {"partitions": partitions}
