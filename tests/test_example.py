from datetime import datetime, timedelta, timezone

import icechunk
from icechunk import Repository, Session
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.typing import VirtualizarrProcessor


def protocol_type_check(processor: VirtualizarrProcessor) -> None:
    assert processor


def test_follows_protocol() -> None:
    processor = Processor()
    protocol_type_check(processor=processor)


def test_initialize_repo() -> None:
    processor = Processor()
    result = processor.initialize_repo()
    assert isinstance(result, Repository)


def test_process_file(icechunk_session: Session) -> None:
    processor = Processor()
    result = processor.process_file(file_key="2024-01-02", session=icechunk_session)
    assert result


def test_commit_processed_files(icechunk_session: Session) -> None:
    processor = Processor()
    snapshot = processor.commit_processed_files(session=icechunk_session)
    assert isinstance(snapshot, str)


def test_garbage_collect() -> None:
    processor = Processor()
    expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
    gcs = processor.garbage_collect(expiry_time=expiry_time)
    assert isinstance(gcs, icechunk.GCSummary)
