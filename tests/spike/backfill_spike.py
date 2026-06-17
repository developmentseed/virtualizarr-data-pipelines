"""Disposable spike: Icechunk coordinator-creates-forks distributed-write cycle.

See docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md.
This is throwaway proof-of-mechanics code, not production code.
"""

import os

import icechunk
import numpy as np

# Synthetic array: N time steps, each a (Y, X) int32 chunk. One chunk per time step.
N, Y, X = 6, 2, 3
DTYPE = np.dtype("int32")
CHUNK_NBYTES = Y * X * DTYPE.itemsize


def _chunks_dir(work: str) -> str:
    return os.path.join(work, "chunks")


def _repo_dir(work: str) -> str:
    return os.path.join(work, "repo")


def _url_prefix(work: str) -> str:
    return f"file://{_chunks_dir(work)}/"


def _source_path(work: str) -> str:
    return f"{_chunks_dir(work)}/source.bin"


def _source_url(work: str) -> str:
    return f"file://{_source_path(work)}"


def open_repo(work: str) -> icechunk.Repository:
    """Open (or create) the spike repo on local-filesystem storage with a virtual
    chunk container authorizing the local source file."""
    os.makedirs(_chunks_dir(work), exist_ok=True)
    chunk_store = icechunk.local_filesystem_store(_chunks_dir(work))
    storage = icechunk.local_filesystem_storage(_repo_dir(work))
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(_url_prefix(work), chunk_store)
    )
    return icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access={_url_prefix(work): None},
    )
