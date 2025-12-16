from datetime import datetime, timedelta, timezone

import icechunk
from icechunk import Repository
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.typing import VirtualizarrProcessor


def protocol_type_check(processor: VirtualizarrProcessor) -> None:
    assert processor


def test_follows_protocol() -> None:
    processor = Processor()
    protocol_type_check(processor=processor)


def test_initialize_store() -> None:
    processor = Processor()
    result = processor.initialize_store()
    assert isinstance(result, Repository)


def test_process_file() -> None:
    processor = Processor()
    snapshot = processor.process_file(file_key="2024-01-02")
    assert isinstance(snapshot, str)


def test_garbage_collect() -> None:
    processor = Processor()
    expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
    gcs = processor.garbage_collect(expiry_time=expiry_time)
    assert isinstance(gcs, icechunk.GCSummary)
