import logging
import os
import tempfile
from datetime import datetime
from itertools import islice
from typing import Any, NamedTuple, cast

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

# Forward synthetic dataset: one time step per file, each a (Y, X) chunk.
FORWARD_Y, FORWARD_X = 2, 3

# Backfill synthetic dataset: N time steps, each a (Y, X) int32 chunk.
BACKFILL_N, BACKFILL_Y, BACKFILL_X = 6, 2, 3
BACKFILL_DTYPE = np.dtype("int32")

# The dimension forward processing extends. Every reference here goes through
# this name, so an implementation with a differently named axis only changes it.
APPEND_DIMENSION = "time"


class WritePlan(NamedTuple):
    """How one file gets written.

    `plan.dataset.vz.to_icechunk(store, **plan.kwargs)`.
    """

    mode: str  # "create" | "region" | "append"
    dataset: xr.Dataset
    kwargs: dict[str, Any]


def store_append_dimension(
    store: Any, dimension: str = APPEND_DIMENSION
) -> np.ndarray | None:
    """The coordinate values a store already holds along `dimension`, or None if
    it holds no data yet.

    Re-read for every file rather than cached: a write earlier in the same batch
    moves the axis that the next file has to see. A session reads its own
    uncommitted writes, so this stays correct mid-batch.
    """
    try:
        dataset = xr.open_zarr(store, consolidated=False, zarr_format=3)
    except Exception:
        # No group at all yet: a forward-only deployment before its first file.
        return None
    if dimension not in dataset.coords:
        return None
    return cast(np.ndarray, dataset[dimension].values)


def write_plan(
    dataset: xr.Dataset,
    existing: np.ndarray | None,
    dimension: str = APPEND_DIMENSION,
) -> WritePlan:
    """Choose between creating, region-writing and appending one file.

    The deciding question is whether the store's axis already carries this
    file's coordinate:

    * **region** -- it does, so the row exists and must be written in place.
      An append would add a second row with the same coordinate value, leaving
      the axis non-monotonic and the file stored twice. This is the normal case
      after a backfill: the store is declared at its full extent up front, so
      every coordinate inside that extent already has a (possibly empty) row
      waiting, and it is also how a re-delivered notification lands harmlessly.
    * **append** -- it does not, which is the forward case: a coordinate past
      the declared axis extends it by one row.
    * **create** -- there is no array at all yet, the first file of a
      forward-only deployment, where the write has to create the store.

    An implementation whose dataset carries variables without the append
    dimension (a static grid coordinate, say) has to drop them from the region
    dataset: `region="auto"` resolves a slice per dimension and has none to
    resolve for those. `plan.dataset` is the hook for that -- return the
    reduced dataset for the region mode and the full one for an append, where
    having no append dimension means the already-written copies are left alone.
    """
    if existing is None:
        return WritePlan("create", dataset, {})
    coordinate = dataset[dimension].values[0]
    if bool((existing == coordinate).any()):
        return WritePlan("region", dataset, {"region": "auto"})
    return WritePlan("append", dataset, {"append_dim": dimension})


def synthetic_vds(date: str) -> xr.Dataset:
    """One file as a virtual dataset: a single (Y, X) chunk on a leading
    `time` dimension.

    The data variable carries the append dimension rather than only the
    coordinate carrying it, so a file whose coordinate is already on the axis
    has a row to be written into by `region="auto"`. This matches the shape
    `initialize_backfill_store` declares.
    """
    filepath = f"{CHUNK_DIR}/data_chunk"
    store = obstore.store.LocalStore()
    arr = np.repeat([[1, 2]], 3, axis=1).reshape(1, FORWARD_Y, FORWARD_X)
    shape = arr.shape
    dtype = arr.dtype
    buf = arr.tobytes()
    obstore.put(
        store,
        filepath,
        buf,
    )
    manifest = ChunkManifest(
        {"0.0.0": {"path": filepath, "offset": 0, "length": len(buf)}}
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
        dimension_names=(APPEND_DIMENSION, "y", "x"),
        storage_transformers=None,
    )
    ma = ManifestArray(
        chunkmanifest=manifest,
        metadata=metadata,
    )
    foo = xr.Variable(
        data=ma, dims=[APPEND_DIMENSION, "y", "x"], encoding={"scale_factor": 2}
    )
    vds = xr.Dataset(
        {"foo": foo},
        coords={
            APPEND_DIMENSION: (
                APPEND_DIMENSION,
                [np.datetime64(date)],
            )  # Single time point
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
        """Write one file into `main`, creating, region-writing or appending.

        Which of the three depends on the file: one whose coordinate is already
        on the store's axis is written in place, one that is not extends the
        axis by a row. `write_plan` holds the reasoning.
        """
        try:
            vds = synthetic_vds(file_key)
            plan = write_plan(vds, store_append_dimension(session.store))
            logger.info("%s: %s write to main", file_key, plan.mode)
            plan.dataset.vz.to_icechunk(
                session.store, validate_containers=False, **plan.kwargs
            )
            return True
        except Exception:
            logger.exception("process_file failed for %s", file_key)
            return False

    def commit_processed_files(self, session: Session) -> str:
        snapshot = session.commit(message=f"Update {session.snapshot_id}")
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
