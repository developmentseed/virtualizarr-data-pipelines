"""Forward handler: a batch is one commit, with two distinct failure modes.

The contract these pin down is the ordering. A per-file failure is reported to
SQS only *after* the commit has succeeded, so the records left off the failure
list really are stored. A commit failure is raised instead, because then nothing
is stored and the whole batch has to come back.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

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


def make_processor(MockProcessor: MagicMock) -> MagicMock:
    processor = MockProcessor.return_value
    processor.initialize_repo.return_value = MagicMock()
    session = MagicMock()
    session.has_uncommitted_changes = True
    processor.initialize_session.return_value = session
    processor.process_file.return_value = True
    processor.commit_processed_files.return_value = "snapshot-123"
    return processor


@patch("process_messages.handler.Processor")
def test_a_batch_that_writes_and_commits_reports_no_failures(
    MockProcessor: MagicMock,
) -> None:
    processor = make_processor(MockProcessor)

    response = handler(make_sqs_event(["2024-01-02", "2024-01-03"]), MagicMock())

    assert response == {"batchItemFailures": []}
    assert [c.kwargs["file_key"] for c in processor.process_file.call_args_list] == [
        "2024-01-02",
        "2024-01-03",
    ]
    # one session shared across the batch, one commit at the end
    processor.initialize_session.assert_called_once()
    processor.commit_processed_files.assert_called_once()


@patch("process_messages.handler.Processor")
def test_one_failed_file_returns_only_its_own_message(MockProcessor: MagicMock) -> None:
    """The rest of the batch still commits, so those files really are stored and
    only the failed message needs redelivering."""
    processor = make_processor(MockProcessor)
    processor.process_file.side_effect = [True, False, True]

    response = handler(
        make_sqs_event(["2024-01-02", "unwritable-key", "2024-01-04"]), MagicMock()
    )

    assert response == {"batchItemFailures": [{"itemIdentifier": "msg-001"}]}
    processor.commit_processed_files.assert_called_once()


@patch("process_messages.handler.Processor")
def test_a_file_that_raises_is_held_rather_than_aborting_the_batch(
    MockProcessor: MagicMock,
) -> None:
    processor = make_processor(MockProcessor)
    processor.process_file.side_effect = [Exception("boom"), True]

    response = handler(make_sqs_event(["bad-key", "2024-01-03"]), MagicMock())

    assert response == {"batchItemFailures": [{"itemIdentifier": "msg-000"}]}
    processor.commit_processed_files.assert_called_once()


@patch("process_messages.handler.Processor")
def test_a_failed_commit_fails_the_whole_invocation(MockProcessor: MagicMock) -> None:
    """Files written into a session that never commits have not been stored.
    Reporting failures here would delete their messages; raising redelivers
    every one of them."""
    processor = make_processor(MockProcessor)
    processor.commit_processed_files.side_effect = Exception("conflict")

    with pytest.raises(Exception, match="conflict"):
        handler(make_sqs_event(["2024-01-02", "2024-01-03"]), MagicMock())


@patch("process_messages.handler.Processor")
def test_a_batch_where_nothing_could_be_written_fails_the_invocation(
    MockProcessor: MagicMock,
) -> None:
    """There is no commit to hang a partial failure report off, and Icechunk
    refuses an empty commit, so the batch goes back whole."""
    processor = make_processor(MockProcessor)
    processor.process_file.return_value = False
    processor.initialize_session.return_value.has_uncommitted_changes = False

    with pytest.raises(RuntimeError, match="no file in this batch could be written"):
        handler(make_sqs_event(["bad-a", "bad-b"]), MagicMock())

    processor.commit_processed_files.assert_not_called()


@patch("process_messages.handler.Processor")
def test_a_store_that_cannot_be_opened_is_logged_before_it_raises(
    MockProcessor: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """A misconfigured deployment fails here -- no bucket, no permission on it --
    and it happens before any per-file logging. Unguarded it leaves nothing but
    the runtime's plain-text traceback, which is invisible to a level filter."""
    MockProcessor.return_value.initialize_repo.side_effect = KeyError(
        "ICECHUNK_LOCAL_PATH"
    )

    with caplog.at_level("ERROR"):
        with pytest.raises(KeyError):
            handler(make_sqs_event(["2024-01-02"]), MagicMock())

    assert "Could not open the store" in caplog.text


@patch("process_messages.handler.Processor")
def test_an_sns_wrapped_message_is_unwrapped(MockProcessor: MagicMock) -> None:
    processor = make_processor(MockProcessor)
    inner = {
        "Records": [
            {"s3": {"bucket": {"name": "test-bucket"}, "object": {"key": "wrapped"}}}
        ]
    }
    event = make_sqs_event(["placeholder"])
    event["Records"][0]["body"] = json.dumps({"Message": json.dumps(inner)})

    handler(event, MagicMock())

    assert processor.process_file.call_args.kwargs["file_key"] == "wrapped"
