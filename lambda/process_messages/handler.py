"""Forward ingest: a batch is one Icechunk commit, with two failure modes.

Every record in a batch writes into a single shared session, and nothing is
durable until that session commits at the end. That asymmetry decides how each
kind of failure is reported:

* **one file fails** -- the rest of the batch still commits, so those files are
  genuinely stored and only the failed message needs to come back. It is
  returned as a batch item failure, and SQS redelivers just that one.
* **the commit fails** -- nothing is durable, including the files that wrote
  without complaint. The handler raises, Lambda records the invocation as
  failed, and SQS redelivers every message in the batch.

The ordering is what makes the first case honest: the failure list is only ever
returned *after* a successful commit. Reporting per-record success before the
commit would delete messages for files that were never stored, which is what an
earlier version of this handler did.

A redelivered batch re-runs against a fresh session, which is safe because
`write_plan` recomputes each cycle's placement from the axis it finds: a cycle
another invocation has since written comes back as a region write, not an append.
"""

import json
from typing import Any, Dict

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.logging import utils
from aws_lambda_powertools.utilities.batch.types import (
    PartialItemFailureResponse,
    PartialItemFailures,
)
from aws_lambda_powertools.utilities.data_classes import SQSEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
from icechunk import Session
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()

# The processor reports a per-file failure by returning False and logging the
# real cause through its own module logger, which is not in Powertools'
# structured format by default and is easy to miss. Adopting it here puts those
# tracebacks in the same JSON stream as everything else.
utils.copy_config_to_registered_loggers(source_logger=logger)


@tracer.capture_method
def process_notification(
    message: Dict[str, Any],
    session: Session,
    processor: Processor,
) -> None:
    """Write the file named by one S3 notification into the shared session."""
    bucket = message.get("Records", [{}])[0].get("s3", {}).get("bucket", {}).get("name")
    key = message.get("Records", [{}])[0].get("s3", {}).get("object", {}).get("key")
    if not (key and bucket):
        logger.warning("Notification carried no S3 object; nothing to write")
        return

    s3_uri = f"s3://{bucket}/{key}"
    logger.info("Process file", extra={"bucket": bucket, "key": key, "s3_uri": s3_uri})

    # `process_file` reports failure by returning False rather than raising, so
    # that has to be turned back into an exception here -- otherwise the batch
    # carries on and commits as though the file had been written.
    if not processor.process_file(file_key=key, session=session):
        raise RuntimeError(f"process_file failed for {s3_uri}; see the traceback above")
    logger.info("Wrote file into the pending commit", extra={"s3_uri": s3_uri})


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> PartialItemFailureResponse:
    """Write every file in the batch, commit once, then report what failed."""
    # Inside the try from the first line: opening the repo is where a
    # misconfigured deployment fails -- no ICECHUNK_BUCKET, no permission on it,
    # a store that cannot be created -- and an exception raised before the first
    # structured line leaves nothing behind but the runtime's plain-text
    # traceback, which is invisible to anything filtering these logs by level.
    try:
        sqs_event = SQSEvent(event)
        processor = Processor()
        session = processor.initialize_session(repo=processor.initialize_repo())
        records = list(sqs_event.records)
    except Exception:
        logger.exception("Could not open the store; the batch returns to the queue")
        raise

    failures: list[PartialItemFailures] = []

    for record in records:
        try:
            message = json.loads(record.body)
            # Unwrap the SNS envelope when the queue is subscribed to a topic.
            if "Message" in message:
                message = json.loads(message["Message"])
            process_notification(message=message, session=session, processor=processor)
        except Exception:
            # Held, not raised: the rest of the batch can still be committed,
            # and this message is reported for redelivery once it has been.
            logger.exception(
                "File failed; its message will be returned to the queue",
                extra={"message_id": record.message_id},
            )
            failures.append({"itemIdentifier": record.message_id})

    if not session.has_uncommitted_changes:
        # Nothing reached the store. There is no commit to make, and Icechunk
        # would refuse an empty one anyway, so raise rather than report failures
        # piecemeal -- every message in the batch has to come back.
        if failures:
            # Each failure already logged its own cause above; this line records
            # the batch-level outcome in the same structured stream, so the
            # reason the invocation died is not left to Lambda's plain-text
            # unhandled-exception output.
            logger.error(
                "No file in this batch could be written; returning the whole batch",
                extra={"failed": len(failures), "records": len(records)},
            )
            raise RuntimeError(
                f"no file in this batch could be written "
                f"({len(failures)} of {len(records)} failed); returning the batch"
            )
        logger.info("Batch carried no writable files; nothing to commit")
        return {"batchItemFailures": []}

    try:
        snapshot_id = processor.commit_processed_files(session=session)
    except Exception:
        # Raised, not reported: without the commit even the files that wrote
        # cleanly are not stored, so the whole batch must be redelivered.
        logger.exception(
            "Commit failed; nothing was stored and every message returns to the queue"
        )
        raise

    logger.info(
        "Committed batch",
        extra={
            "snapshot_id": snapshot_id,
            "written": len(records) - len(failures),
            "failed": len(failures),
        },
    )
    # Only reachable once the commit has succeeded, which is what makes it safe
    # to let the records that are not listed here be deleted.
    return {"batchItemFailures": failures}
