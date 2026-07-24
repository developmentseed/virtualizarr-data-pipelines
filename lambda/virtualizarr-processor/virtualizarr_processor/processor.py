import logging
import os
import tempfile
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

logger = logging.getLogger(__name__)

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"

# Backfill synthetic dataset: N time steps, each a (Y, X) int32 chunk.
BACKFILL_N, BACKFILL_Y, BACKFILL_X = 6, 2, 3
BACKFILL_DTYPE = np.dtype("int32")


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
            authorize_virtual_chunk_access={
                CHUNK_DIRECTORY_URL_PREFIX: icechunk.credentials.LocalFileSystemAccess
            },
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
        time_coord = root.create_array(
            "time",
            shape=(BACKFILL_N,),
            chunks=(BACKFILL_N,),
            dtype="int64",
            dimension_names=("time",),
        )
        time_coord[:] = np.arange(BACKFILL_N)
        return cast(str, session.commit("Initialize backfill shape"))

    def open_backfill_repo(self) -> Repository:
        # Reference impl storage config, read from the environment:
        #   ICECHUNK_BUCKET  - if set, use S3 storage (Lambda); IAM creds via from_env
        #   ICECHUNK_PREFIX  - S3 key prefix (optional)
        #   ICECHUNK_REGION  - S3 region (optional)
        #   ICECHUNK_LOCAL_PATH - filesystem repo path when no bucket (tests)
        chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
        bucket = os.environ.get("ICECHUNK_BUCKET")
        if bucket:
            storage = icechunk.s3_storage(
                bucket=bucket,
                prefix=os.environ.get("ICECHUNK_PREFIX"),
                region=os.environ.get("ICECHUNK_REGION"),
                from_env=True,
            )
        else:
            storage = icechunk.local_filesystem_storage(
                os.environ["ICECHUNK_LOCAL_PATH"]
            )
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
        )
        return icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access={
                CHUNK_DIRECTORY_URL_PREFIX: icechunk.credentials.LocalFileSystemAccess
            },
        )

    def _backfill_slice_vds(self, t: int) -> xr.Dataset:
        """A one-time-step virtual dataset for backfill index t, carrying the
        matching `time` coordinate so to_icechunk(region="auto") can place it."""
        buf = np.full((1, BACKFILL_Y, BACKFILL_X), t, dtype=BACKFILL_DTYPE).tobytes()
        # Synthetic reference only: each slice writes its own local source chunk.
        # These accumulate under CHUNK_DIR; a real Processor references existing
        # source files (e.g. in S3) and does not create per-slice temp files.
        filepath = f"{CHUNK_DIR}/backfill_slice_{t}"
        obstore.put(obstore.store.LocalStore(), filepath, buf)
        manifest = ChunkManifest(
            {"0.0.0": {"path": filepath, "offset": 0, "length": len(buf)}}
        )
        zdtype = parse_data_type(BACKFILL_DTYPE, zarr_format=3)
        metadata = ArrayV3Metadata(
            shape=(1, BACKFILL_Y, BACKFILL_X),
            data_type=zdtype,
            chunk_grid={
                "name": "regular",
                "configuration": {"chunk_shape": (1, BACKFILL_Y, BACKFILL_X)},
            },
            chunk_key_encoding={"name": "default"},
            fill_value=zdtype.default_scalar(),
            codecs=[BytesCodec()],
            attributes={},
            dimension_names=("time", "y", "x"),
            storage_transformers=None,
        )
        ma = ManifestArray(chunkmanifest=manifest, metadata=metadata)
        return xr.Dataset(
            {"foo": xr.Variable(("time", "y", "x"), ma)},
            coords={"time": ("time", [t])},
        )

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        try:
            # Synthetic keys are the integer time index as a string ("0".."5").
            # A real processor parses the source file for its own coordinate.
            t = int(file_key)
            self._backfill_slice_vds(t).vz.to_icechunk(
                fork.store, region="auto", validate_containers=False
            )
            return True
        except Exception:
            # Catch parse/region errors and I/O failures from to_icechunk, but log
            # the real cause first — otherwise the worker only reports a generic
            # "process_backfill_file failed" and the underlying error is lost.
            # A real (network-reading) processor should also retry the granule read
            # here with backoff, since transient object-store / auth throttling under
            # a large backfill's concurrency is otherwise fatal. logger.exception
            # includes the traceback.
            logger.exception("process_backfill_file failed for %s", file_key)
            return False

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.initialize_repo()
        repo.expire_snapshots(older_than=expiry_time)
        gcs = repo.garbage_collect(delete_object_older_than=expiry_time)
        return gcs
