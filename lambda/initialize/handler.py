from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> None:
    try:
        virtualizarr_processor = Processor()
        virtualizarr_processor.initialize_store()
        logger.info("Icechunk initialized")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
