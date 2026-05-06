import json
from typing import Any, Dict

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
)
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.data_classes import SQSEvent, SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from icechunk import Session
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()
batch_processor = BatchProcessor(event_type=EventType.SQS)


@tracer.capture_method
def process_notification(
    message: Dict[str, Any],
    session: Session,
    processor: Processor,
) -> None:
    """
    Process a notification message.

    Args:
        message: The notification message to process
    """
    # Extract key information from the message
    bucket = message.get("Records", [{}])[0].get("s3", {}).get("bucket", {}).get("name")
    key = message.get("Records", [{}])[0].get("s3", {}).get("object", {}).get("key")
    if key and bucket:
        s3_uri = f"s3://{bucket}/{key}"
        logger.info(
            "Append file",
            extra={"bucket": bucket, "key": key, "s3_uri": s3_uri},
        )
        processor.process_file(file_key=key, session=session)
        logger.info(f"{s3_uri} successfully processed")


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> PartialItemFailureResponse:
    """
    Lambda function to process notification messages from SQS queue.

    Args:
        event: Lambda event containing SQS records
        context: Lambda context object

    """
    sqs_event = SQSEvent(event)
    records = sqs_event.raw_event["Records"]
    virtualizarr_processor = Processor()
    repo = virtualizarr_processor.initialize_repo()
    session = virtualizarr_processor.initialize_session(repo=repo)

    @tracer.capture_method
    def record_handler(record: SQSRecord) -> None:
        """
        Process individual SQS record.

        Args:
            record: SQS record from the batch
        """
        try:
            # Extract message body
            message_body = record.body

            # Parse the SNS message if it's from SNS
            message = json.loads(message_body)

            # If message is from SNS, extract the actual message
            if "Message" in message:
                sns_message = json.loads(message["Message"])
                process_notification(
                    message=sns_message,
                    session=session,
                    processor=virtualizarr_processor,
                )
            else:
                # Direct SQS message
                process_notification(
                    message=message,
                    session=session,
                    processor=virtualizarr_processor,
                )

        except Exception as e:
            logger.error(
                f"Error processing record: {str(e)}",
                extra={"message_id": record.message_id},
            )
            raise

    # Process each record individually
    with batch_processor(records=records, handler=record_handler) as batch:
        batch.process()
    # Now attempt the commit:
    try:
        snapshot_id = virtualizarr_processor.commit_processed_files(session=session)
        logger.info(f"Committed to {snapshot_id}")
    except Exception:
        logger.error("Commit failed, marking all records as failed")
        return {
            "batchItemFailures": [
                {"itemIdentifier": record["messageId"]} for record in records
            ]
        }

    # Commit succeeded — return normal partial failure response
    # (only individually-failed records retry)
    return batch_processor.response()
