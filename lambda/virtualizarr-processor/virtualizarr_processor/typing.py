from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol, runtime_checkable

import icechunk
from icechunk import ForkSession, Repository, Session


@runtime_checkable
class VirtualizarrProcessor(Protocol):
    def initialize_repo(self) -> Repository:
        """
        Initialize an Icechunk Store with the necessary structure and return
        a Repository handle.

        This store should have a dimension that can be used with an append function.

        Parameters
        ----------

        Returns
        -------
        Repository
            An Icechunk Repository.
        """
        ...

    def initialize_session(self, repo: Repository) -> Session:
        """
        Initialize an Icechunk writable Session.

        Parameters
        ----------
            repo: An Icechunk Repository.
        Returns
        -------
        Session
            An Icechunk writable Session.
        """
        ...

    def process_file(self, file_key: str, session: Session) -> bool:
        """
        Uses a Virtualizarr parser to parse the file, manipulate the resulting
        ManifestStore and add it to the Icechunk store

        Parameters
        ----------
            file_key: The full key path to the source file.
            session: The Icechunk writable Session to use for adding the file.
        Returns
        -------
        bool
            True if file was successfully processed.
        """
        ...

    def commit_processed_files(self, session: Session) -> str:
        """
        Commits the updates made by one or multiple calls to process_file

        Parameters
        ----------
            session: The Icechunk writable Session used with process_file.
        Returns
        -------
        str
            A snapshot id of the append commit.
        """
        ...

    def initialize_backfill_store(self, repo: Repository) -> str:
        """
        Create the `backfill` branch off the current `main` tip and build the
        full-shape array(s) and coordinates (metadata only), commit, and return
        the base snapshot id.

        The store is declared at its full extent up front because backfill writes
        disjoint regions via set_virtual_ref rather than appending. The session
        MUST have no uncommitted changes after this returns, so that forks taken
        from a fresh session share the committed branch-tip snapshot as their base.

        The `backfill` branch must not already exist. This method is intended to
        be called exactly once per backfill run.

        Parameters
        ----------
            repo: An Icechunk Repository (durable storage; not in-memory).
        Returns
        -------
        str
            The base snapshot id of the committed full-shape store.
        """
        ...

    def region_for(self, file_key: str) -> Mapping[str, int]:
        """
        Map a file key to its absolute index/region in the pre-sized array.

        Must be deterministic and side-effect-free so the partitioner can call it
        to assign and verify disjoint partitions.

        Parameters
        ----------
            file_key: An identifier for the source file. The scheme is up to the
                implementation (e.g. an S3 object key, a date string, or an
                integer index) as long as it maps deterministically to a region.
        Returns
        -------
        Mapping[str, int]
            A per-dimension index map, e.g. {"time": 42}.
        """
        ...

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        """
        Write the file's virtual references into the fork's store at
        region_for(file_key) via set_virtual_ref. Must NOT commit.

        Parameters
        ----------
            file_key: The full key path to the source file.
            fork: An Icechunk ForkSession to write references into.
        Returns
        -------
        bool
            True if the file was successfully processed.
        """
        ...

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        """
        Run Icechunk garbage collection and snapshot removal.

        Parameters
        ----------
            repo: And Icechunk Repository.
            expiry_time: Remove snapshots older than this time.
        Returns
        -------
        GCSummary
        """
        ...

    # def cron_processing(self, store: IcechunkStore) -> str:
    # """
    # Variable level operations that need to be run periodically and then
    # released as a tag.

    # Parameters
    # ----------
    # store: And Icechunk store.
    # Returns
    # -------
    # str
    # """
    # ...
