import os
import tempfile
from collections.abc import Mapping
from copy import Error
from datetime import datetime
from itertools import islice
from typing import cast

import icechunk
import numpy as np
import obstore
import xarray as xr
import zarr
from icechunk import ForkSession, Repository, Session
from virtualizarr.manifests import ChunkManifest, ManifestArray
from zarr.codecs import BytesCodec
from zarr.core.dtype import parse_data_type
from zarr.core.metadata import ArrayV3Metadata

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"

# Backfill synthetic dataset: N time steps, each a (Y, X) int32 chunk.
BACKFILL_N, BACKFILL_Y, BACKFILL_X = 6, 2, 3
BACKFILL_DTYPE = np.dtype("int32")
BACKFILL_CHUNK_NBYTES = BACKFILL_Y * BACKFILL_X * BACKFILL_DTYPE.itemsize
BACKFILL_SOURCE_PATH = f"{CHUNK_DIR}/backfill_source.bin"
BACKFILL_SOURCE_URL = f"file://{BACKFILL_SOURCE_PATH}"


def synthetic_vds(date: str) -> xr.Dataset:
    filepath = f"{CHUNK_DIR}/data_chunk"
    store = obstore.store.LocalStore()
    arr = np.repeat([[1, 2]], 3, axis=1)
    shape = arr.shape
    dtype = arr.dtype
    buf = arr.tobytes()
    obstore.put(
        store,
        filepath,
        buf,
    )
    manifest = ChunkManifest(
        {"0.0": {"path": filepath, "offset": 0, "length": len(buf)}}
    )
    zdtype = parse_data_type(dtype, zarr_format=3)
    metadata = ArrayV3Metadata(
        shape=shape,
        data_type=zdtype,
        chunk_grid={
            "name": "regular",
            "configuration": {"chunk_shape": shape},
        },
        chunk_key_encoding={"name": "default"},
        fill_value=zdtype.default_scalar(),
        codecs=[BytesCodec()],
        attributes={},
        dimension_names=("y", "x"),
        storage_transformers=None,
    )
    ma = ManifestArray(
        chunkmanifest=manifest,
        metadata=metadata,
    )
    foo = xr.Variable(data=ma, dims=["y", "x"], encoding={"scale_factor": 2})
    vds = xr.Dataset(
        {"foo": foo},
        coords={
            "time": ("time", [np.datetime64(date)])  # Single time point
        },
    )
    return vds


class Processor:
    def initialize_repo(self) -> Repository:
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
        # Get only up to 2 commits to check if the repository is new
        history = list(islice(repo.ancestry(branch="main"), 2))
        if len(history) == 1:
            session = repo.writable_session("main")
            vds = synthetic_vds("2024-01-01")
            vds.vz.to_icechunk(session.store, validate_containers=False)
            session.commit(message="Initialization")
        return repo

    def initialize_session(self, repo: Repository) -> Session:
        session = repo.writable_session("main")
        return session

    def process_file(self, file_key: str, session: Session) -> bool:
        result = False
        try:
            vds = synthetic_vds(file_key)
            vds.vz.to_icechunk(
                session.store, append_dim="time", validate_containers=False
            )
            result = True
        except Error:
            result = False
        return result

    def commit_processed_files(self, session: Session) -> str:
        snapshot = session.commit(message=f"Append to {session.snapshot_id}")
        return str(snapshot)

    def initialize_backfill_store(self, repo: Repository) -> str:
        # Write the synthetic source: N back-to-back chunks, chunk t filled with
        # value t, so chunk t is at byte offset t * BACKFILL_CHUNK_NBYTES.
        buf = b"".join(
            np.full((BACKFILL_Y, BACKFILL_X), t, dtype=BACKFILL_DTYPE).tobytes()
            for t in range(BACKFILL_N)
        )
        obstore.put(obstore.store.LocalStore(), BACKFILL_SOURCE_PATH, buf)

        repo.create_branch("backfill", repo.lookup_branch("main"))
        session = repo.writable_session("backfill")
        root = zarr.open_group(session.store, mode="a")
        root.create_array(
            "foo",
            shape=(BACKFILL_N, BACKFILL_Y, BACKFILL_X),
            chunks=(1, BACKFILL_Y, BACKFILL_X),
            dtype=BACKFILL_DTYPE,
            serializer=BytesCodec(),
            compressors=None,
            filters=None,
            dimension_names=("time", "y", "x"),
        )
        return cast(str, session.commit("Initialize backfill shape"))

    def region_for(self, file_key: str) -> Mapping[str, int]:
        # Synthetic keys are the integer time index as a string ("0".."5").
        # Real implementations would parse their own scheme (e.g. a date).
        return {"time": int(file_key)}

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        try:
            t = self.region_for(file_key)["time"]
            fork.store.set_virtual_ref(
                f"foo/c/{t}/0/0",
                BACKFILL_SOURCE_URL,
                offset=t * BACKFILL_CHUNK_NBYTES,
                length=BACKFILL_CHUNK_NBYTES,
                validate_container=False,
            )
            return True
        except Exception:
            # Catch parse/region errors and I/O failures from set_virtual_ref,
            # returning False to mirror process_file's bool contract. (Broader
            # than process_file's `except Error` because set_virtual_ref can
            # raise icechunk/object-store errors, not just copy.Error.)
            return False

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.initialize_repo()
        repo.expire_snapshots(older_than=expiry_time)
        gcs = repo.garbage_collect(delete_object_older_than=expiry_time)
        return gcs
