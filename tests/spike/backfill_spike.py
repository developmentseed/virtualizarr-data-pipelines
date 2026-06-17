"""Disposable spike: Icechunk coordinator-creates-forks distributed-write cycle.

See docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md.
This is throwaway proof-of-mechanics code, not production code.
"""

import multiprocessing as mp
import os
import pickle
from typing import cast

import icechunk
import numpy as np
import obstore
import zarr
from zarr.codecs import BytesCodec

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


def write_source(work: str) -> None:
    """Write one source file holding N back-to-back chunk buffers; chunk t is filled
    with the value t, so chunk t lives at byte offset t * CHUNK_NBYTES."""
    buf = b"".join(np.full((Y, X), t, dtype=DTYPE).tobytes() for t in range(N))
    obstore.put(obstore.store.LocalStore(), _source_path(work), buf)


def init_backfill_store(repo: icechunk.Repository, work: str) -> None:
    """Create the `backfill` branch off main and the full-shape `foo` array
    (metadata only — no chunks written yet)."""
    repo.create_branch("backfill", repo.lookup_branch("main"))
    session = repo.writable_session("backfill")
    root = zarr.open_group(session.store, mode="a")
    root.create_array(
        "foo",
        shape=(N, Y, X),
        chunks=(1, Y, X),
        dtype=DTYPE,
        serializer=BytesCodec(),
        compressors=None,
        filters=None,
        dimension_names=("time", "y", "x"),
    )
    session.commit("Initialize backfill shape")


def run_worker(
    in_path: str, indices: list[int], source_url: str, out_path: str
) -> None:
    """Worker body. Runs in a separate (spawned) process. Loads the coordinator-made
    fork, writes a virtual chunk reference for each assigned time index, and pickles
    the fork back. Does NOT open the repo — the pickled fork carries everything."""
    with open(in_path, "rb") as f:
        fork = pickle.loads(f.read())
    for t in indices:
        fork.store.set_virtual_ref(
            f"foo/c/{t}/0/0",
            source_url,
            offset=t * CHUNK_NBYTES,
            length=CHUNK_NBYTES,
            validate_container=False,
        )
    with open(out_path, "wb") as f:
        f.write(pickle.dumps(fork))


def run_backfill(repo: icechunk.Repository, work: str, subsets: list[list[int]]) -> str:
    """Coordinator. Opens one writable session, forks once per worker subset, spawns
    a worker process per fork, then discovers the returned forks by listing the output
    folder, merges them into the same session, and commits once. Returns the new tip."""
    forks_in = os.path.join(work, "forks_in")
    forks_out = os.path.join(work, "forks_out")
    os.makedirs(forks_in, exist_ok=True)
    os.makedirs(forks_out, exist_ok=True)

    session = repo.writable_session("backfill")
    ctx = mp.get_context("spawn")
    procs = []
    for i, subset in enumerate(subsets):
        in_path = os.path.join(forks_in, f"worker_{i}.pkl")
        out_path = os.path.join(forks_out, f"worker_{i}.pkl")
        with open(in_path, "wb") as f:
            f.write(pickle.dumps(session.fork()))
        proc = ctx.Process(
            target=run_worker,
            args=(in_path, subset, _source_url(work), out_path),
        )
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()
        if proc.exitcode != 0:
            raise RuntimeError(f"worker exited with {proc.exitcode}")

    # Discovery by folder listing — mirrors a reducer listing an S3 prefix.
    forks = []
    for name in sorted(os.listdir(forks_out)):
        with open(os.path.join(forks_out, name), "rb") as f:
            forks.append(pickle.loads(f.read()))

    session.merge(*forks)
    # cast: pre-commit mypy runs without icechunk, so commit() is Any there and
    # warn_return_any flags a bare return. Do not remove.
    return cast(str, session.commit("Backfill commit"))
