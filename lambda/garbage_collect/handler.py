from datetime import datetime, timedelta, timezone

from aws_lambda_powertools import Logger
from virtualizarr_processor.processor import Processor

logger = Logger()


def handler() -> None:
    try:
        virtualizarr_processor = Processor()
        expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
        print(expiry_time)
        virtualizarr_processor.garbage_collect(expiry_time=expiry_time)
        logger.info("Icechunk garbage collected")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
