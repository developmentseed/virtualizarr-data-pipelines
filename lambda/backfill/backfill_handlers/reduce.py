"""Handler: merge all child forks for a partition into one commit."""

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
    processor = Processor()
    repo = processor.open_backfill_repo()

    child_uris = fork_store.list_forks(event["forks_out_prefix"])
    children = [fork_store.load_fork(uri) for uri in child_uris]
    tip = backfill.merge_and_commit(
        repo, children, message=f"Backfill partition {partition_id}"
    )

    logger.info("Committed partition", extra={"partition_id": partition_id, "tip": tip})
    return {"partition_id": partition_id, "tip": tip}
