import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from aws_lambda_powertools.utilities.batch.exceptions import BatchProcessingError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lambda"))

from process_messages.handler import handler


def make_sqs_event(keys: list[str], bucket: str = "test-bucket") -> dict:
    """Build a minimal SQS event with S3 notification bodies."""
    records = []
    for i, key in enumerate(keys):
        body = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": key},
                    }
                }
            ]
        }
        records.append(
            {
                "messageId": f"msg-{i:03d}",
                "receiptHandle": f"receipt-{i}",
                "body": json.dumps(body),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1717600000000",
                    "ApproximateFirstReceiveTimestamp": "1717600000000",
                },
                "messageAttributes": {},
                "md5OfBody": "abc",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                "awsRegion": "us-east-1",
            }
        )
    return {"Records": records}


@patch("process_messages.handler.Processor")
def test_handler_processes_all_records(MockProcessor: MagicMock) -> None:
    mock_processor = MockProcessor.return_value
    mock_repo = MagicMock()
    mock_session = MagicMock()
    mock_processor.initialize_repo.return_value = mock_repo
    mock_processor.initialize_session.return_value = mock_session
    mock_processor.process_file.return_value = True
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    event = make_sqs_event(["2024-01-02", "2024-01-03"])
    context = MagicMock()

    response = handler(event, context)

    # No failures expected
    assert response["batchItemFailures"] == []

    # Verify process_file was called for each record
    assert mock_processor.process_file.call_count == 2
    calls = mock_processor.process_file.call_args_list
    assert calls[0].kwargs["file_key"] == "2024-01-02"
    assert calls[1].kwargs["file_key"] == "2024-01-03"

    # Verify commit was called once
    mock_processor.commit_processed_files.assert_called_once_with(session=mock_session)


@patch("process_messages.handler.Processor")
def test_handler_raises_when_entire_batch_fails(MockProcessor: MagicMock) -> None:
    """If all records fail, BatchProcessor raises BatchProcessingError."""
    mock_processor = MockProcessor.return_value
    mock_processor.initialize_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.side_effect = Exception("Processing failed")

    event = make_sqs_event(["bad-key"])
    context = MagicMock()

    with pytest.raises(BatchProcessingError):
        handler(event, context)


@patch("process_messages.handler.Processor")
def test_handler_partial_failure(MockProcessor: MagicMock) -> None:
    """If some records fail, only those appear in batchItemFailures."""
    mock_processor = MockProcessor.return_value
    mock_processor.initialize_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.side_effect = [True, Exception("Processing failed")]
    mock_processor.commit_processed_files.return_value = "snapshot-123"

    event = make_sqs_event(["2024-01-02", "bad-key"])
    context = MagicMock()

    response = handler(event, context)

    failed_ids = [item["itemIdentifier"] for item in response["batchItemFailures"]]
    assert "msg-001" in failed_ids
    assert "msg-000" not in failed_ids


@patch("process_messages.handler.Processor")
def test_handler_fails_all_on_commit_error(MockProcessor: MagicMock) -> None:
    """If commit fails, all records should be marked as failed."""
    mock_processor = MockProcessor.return_value
    mock_processor.initialize_repo.return_value = MagicMock()
    mock_processor.initialize_session.return_value = MagicMock()
    mock_processor.process_file.return_value = True
    mock_processor.commit_processed_files.side_effect = Exception("Commit failed")

    event = make_sqs_event(["2024-01-02", "2024-01-03"])
    context = MagicMock()

    response = handler(event, context)

    failed_ids = [item["itemIdentifier"] for item in response["batchItemFailures"]]
    assert "msg-000" in failed_ids
    assert "msg-001" in failed_ids
