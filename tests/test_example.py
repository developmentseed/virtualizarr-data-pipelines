from icechunk import Repository
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.typing import VirtualizarrProcessor


def protocol_type_check(processor: VirtualizarrProcessor) -> None:
    assert processor


# def test_follows_protocol():
# processor = Processor()
# protocol_type_check(processor=processor)


def test_initialize_store() -> None:
    processor = Processor()
    result = processor.initialize_store()
    assert isinstance(result, Repository)


def test_append() -> None:
    processor = Processor()
    snapshot = processor.append(file_key="2024-01-02")
    assert isinstance(snapshot, str)
