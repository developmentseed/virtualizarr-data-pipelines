"""Local-filesystem harness for the Icechunk coordinator-creates-forks
distributed-write cycle, used by test_fork_merge_mechanics.py.

It deliberately uses local filesystem storage and local-file fork blobs (not S3)
so the cross-process test can run real `multiprocessing` spawn workers — moto's
mocked S3 does not cross process boundaries. `run_worker` is the spawn target and
must stay importable as a top-level module (no __init__.py in this directory).

Origin: the fork/merge approach was first proven here as a spike; see
docs/superpowers/specs/2026-06-17-backfill-fork-merge-spike-design.md. The
production graduation lives in virtualizarr_processor (A) and backfill_handlers (B).
"""

import multiprocessing as mp
import os
import pickle
from typing import cast

import icechunk
import numpy as np
import obstore
import virtualizarr  # noqa: F401
import xarray as xr
import zarr
from virtualizarr.manifests import ChunkManifest, ManifestArray
from zarr.codecs import BytesCodec
from zarr.core.dtype import parse_data_type
from zarr.core.metadata import ArrayV3Metadata

# Synthetic array: N time steps, each a (Y, X) int32 chunk. One chunk per time step.
N, Y, X = 6, 2, 3
DTYPE = np.dtype("int32")


def _chunks_dir(work: str) -> str:
    return os.path.join(work, "chunks")


def _repo_dir(work: str) -> str:
    return os.path.join(work, "repo")


def _url_prefix(work: str) -> str:
    return f"file://{_chunks_dir(work)}/"


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
        authorize_virtual_chunk_access={
            _url_prefix(work): icechunk.credentials.LocalFileSystemAccess
        },
    )


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
    time_coord = root.create_array(
        "time", shape=(N,), chunks=(N,), dtype="int64", dimension_names=("time",)
    )
    time_coord[:] = np.arange(N)
    session.commit("Initialize backfill shape")


def _slice_vds_value(work: str, index: int, value: int) -> xr.Dataset:
    """A one-time-step vds at coordinate `index`, filled with `value`."""
    buf = np.full((1, Y, X), value, dtype=DTYPE).tobytes()
    path = f"{_chunks_dir(work)}/slice_{index}_{value}"
    obstore.put(obstore.store.LocalStore(), path, buf)
    manifest = ChunkManifest({"0.0.0": {"path": path, "offset": 0, "length": len(buf)}})
    zdtype = parse_data_type(DTYPE, zarr_format=3)
    metadata = ArrayV3Metadata(
        shape=(1, Y, X),
        data_type=zdtype,
        chunk_grid={"name": "regular", "configuration": {"chunk_shape": (1, Y, X)}},
        chunk_key_encoding={"name": "default"},
        fill_value=zdtype.default_scalar(),
        codecs=[BytesCodec()],
        attributes={},
        dimension_names=("time", "y", "x"),
        storage_transformers=None,
    )
    ma = ManifestArray(chunkmanifest=manifest, metadata=metadata)
    return xr.Dataset(
        {"foo": xr.Variable(("time", "y", "x"), ma)}, coords={"time": ("time", [index])}
    )


def _slice_vds(work: str, t: int) -> xr.Dataset:
    """A one-time-step vds at coordinate t filled with value t."""
    return _slice_vds_value(work, t, t)


def run_worker(in_path: str, indices: list[int], work: str, out_path: str) -> None:
    """Load the coordinator-made fork, region-write each assigned index via
    to_icechunk(region="auto"), pickle the fork back. Runs in a spawned process."""
    with open(in_path, "rb") as f:
        fork = pickle.loads(f.read())
    for t in indices:
        _slice_vds(work, t).vz.to_icechunk(
            fork.store, region="auto", validate_containers=False
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
            args=(in_path, subset, work, out_path),
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
