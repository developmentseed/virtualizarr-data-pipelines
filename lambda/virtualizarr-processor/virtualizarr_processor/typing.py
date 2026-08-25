from __future__ import annotations

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

        The write is not always an append. A store declared at its full extent
        up front -- the normal state after a backfill -- already holds a
        (possibly empty) row for every coordinate inside that extent, and a
        re-delivered notification is a file the store already carries. Appending
        either of those adds a second row with the same coordinate value,
        leaving the axis non-monotonic and the file stored twice; they have to
        be written in place with `region="auto"` instead. Only a coordinate past
        the end of the axis is an append, and only an absent array is a create.

        Choosing between the three is left to the implementation rather than
        expressed as separate Protocol methods, because how a dataset is
        reshaped for a region write depends on the dataset. The reference
        implementation's `write_plan` and `store_append_dimension` in
        `virtualizarr_processor.processor` are a worked example.

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
            A snapshot id of the commit.
        """
        ...

    def initialize_backfill_store(self, repo: Repository) -> str:
        """
        Create the `backfill` branch off the current `main` tip and build the
        full-shape array(s) and coordinates (metadata only), commit, and return
        the base snapshot id.

        The store is declared at its full extent up front because backfill writes
        disjoint regions via region writes rather than appending. It also writes
        the coordinate arrays (e.g. `time`) that region writes rely on to align
        each per-file virtual dataset to the correct position. The session MUST
        have no uncommitted changes after this returns, so that forks taken from
        a fresh session share the committed branch-tip snapshot as their base.

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

    def open_backfill_repo(self) -> Repository:
        """
        Open (or create) the durable backfill repository.

        Storage is chosen by the implementation (e.g. S3 in a deployed Lambda,
        local filesystem in tests). Must use durable, shared storage — a pickled
        ForkSession cannot resolve its base snapshot from in-memory storage.
        Uses open_or_create semantics so the `main` branch exists for
        initialize_backfill_store to branch off.

        Returns
        -------
        Repository
            An Icechunk Repository backed by durable storage.
        """
        ...

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        """
        Write a per-file virtual dataset into the fork's store via
        `vz.to_icechunk(store, region="auto")`, which aligns the dataset to its
        target position by coordinate. Must NOT commit.

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
