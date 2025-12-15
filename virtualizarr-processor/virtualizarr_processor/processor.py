import os
import tempfile

import icechunk
from conftest import synthetic_vds
from icechunk import Repository

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"


class Processor:
    def initialize_store(self) -> Repository:
        chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
        storage = icechunk.in_memory_storage()
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
        )
        repo = icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access={CHUNK_DIRECTORY_URL_PREFIX: None},
        )
        session = repo.writable_session("main")
        vds = synthetic_vds("2024-01-01")
        vds.vz.to_icechunk(session.store, validate_containers=False)
        session.commit(message="Initialization")
        return repo

    def append(self, file_key: str) -> str:
        repo = self.initialize_store()
        session = repo.writable_session("main")
        vds = synthetic_vds(file_key)
        vds.vz.to_icechunk(session.store, append_dim="time", validate_containers=False)
        snapshot = session.commit(message=f"Append {file_key}")
        return str(snapshot)
