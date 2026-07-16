"""Handler: create one shared fork for a partition and write it to S3."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor

from backfill_handlers import fork_store

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    partition_id = event["partition_id"]
    run_prefix = event["run_prefix"]
    fork_in_uri = f"{run_prefix}forks/{partition_id}/in/fork.pkl"
    forks_out_prefix = f"{run_prefix}forks/{partition_id}/out/"

    processor = Processor()
    repo = processor.open_backfill_repo()
    fork_store.save_fork(fork_in_uri, backfill.create_fork(repo))

    logger.info("Created shared fork", extra={"partition_id": partition_id})
    return {
        "partition_id": partition_id,
        "manifest_uri": event["manifest_uri"],
        "fork_in_uri": fork_in_uri,
        "forks_out_prefix": forks_out_prefix,
    }
