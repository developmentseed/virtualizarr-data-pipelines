import os
from datetime import datetime, timedelta, timezone

from aws_lambda_powertools import Logger
from virtualizarr_processor.processor import Processor

logger = Logger()


def handler() -> None:
    try:
        virtualizarr_processor = Processor()
        expiry_hours = int(os.getenv("GARBAGE_COLLECTION_EXPIRY_HOURS", "3"))
        expiry_time = datetime.now(timezone.utc) - timedelta(hours=expiry_hours)
        logger.info(f"Garbage collecting snapshots older than {expiry_time}")
        repo = virtualizarr_processor.initialize_repo()
        virtualizarr_processor.garbage_collect(expiry_time=expiry_time, repo=repo)
        logger.info("Icechunk garbage collected")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
