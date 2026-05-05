from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

import icechunk
from icechunk import Repository, Session


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
