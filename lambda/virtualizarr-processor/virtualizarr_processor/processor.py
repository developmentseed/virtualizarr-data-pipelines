import os
import tempfile
from copy import Error
from datetime import datetime
from itertools import islice

import icechunk
import numpy as np
import obstore
import xarray as xr
from icechunk import Repository, Session
from virtualizarr.manifests import ChunkManifest, ManifestArray
from zarr.codecs import BytesCodec
from zarr.core.dtype import parse_data_type
from zarr.core.metadata import ArrayV3Metadata

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"


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

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.initialize_repo()
        repo.expire_snapshots(older_than=expiry_time)
        gcs = repo.garbage_collect(delete_object_older_than=expiry_time)
        return gcs
