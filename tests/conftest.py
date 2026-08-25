import os
import pathlib
import tempfile

import icechunk
import pytest
import zarr
from virtualizarr_processor.processor import synthetic_vds

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"


def create_repo() -> icechunk.Repository:
    chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
    storage = icechunk.in_memory_storage()
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
    )
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access={
            CHUNK_DIRECTORY_URL_PREFIX: icechunk.credentials.LocalFileSystemAccess
        },
    )
    return repo


def create_session() -> icechunk.Session:
    repo = create_repo()
    session = repo.writable_session("main")
    vds = synthetic_vds("2024-01-01")
    vds.vz.to_icechunk(session.store, validate_containers=False)
    return session


@pytest.fixture(scope="function")
def icechunk_repo() -> icechunk.Repository:
    return create_repo()


@pytest.fixture(scope="function")
def icechunk_session() -> icechunk.Session:
    return create_session()


@pytest.fixture(scope="function")
def backfill_repo(tmp_path: pathlib.Path) -> icechunk.Repository:
    """A filesystem-backed repo with a committed `main` branch.

    Backfill uses durable storage (not in_memory_storage) because a pickled
    ForkSession cannot resolve its base snapshot from an in-memory backing.
    """
    chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
    storage = icechunk.local_filesystem_storage(str(tmp_path / "repo"))
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
    )
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access={
            CHUNK_DIRECTORY_URL_PREFIX: icechunk.credentials.LocalFileSystemAccess
        },
    )
    session = repo.writable_session("main")
    zarr.open_group(session.store, mode="a").create_group("placeholder")
    session.commit("init main")
    return repo
