"""Handler: create the full-shape backfill store and commit (clean base)."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    processor = Processor()
    repo = processor.open_backfill_repo()
    base_snapshot = processor.initialize_backfill_store(repo)
    logger.info("Initialized backfill store", extra={"base_snapshot": base_snapshot})
    return {"base_snapshot": base_snapshot}
