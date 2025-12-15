from __future__ import annotations

from typing import Protocol, runtime_checkable

from icechunk import Repository


@runtime_checkable
class VirtualizarrProcessor(Protocol):
    def initialize_store(self) -> Repository:
        """
        Initialize an IcechunkStore with the necessary structure and return
        a Repository handle.

        This store should have a dimension that can be used append function.

        Parameters
        ----------

        Returns
        -------
        Repository
            An Icechunk Repository to be used in by the processor's append function.
        """
        ...

    def append(self, file_key: str, repo: Repository) -> str:
        """
        Uses a Virtualizarr parser to parse the file, manipulate the resulting
        ManifestStore and append the results along a dimension.

        Parameters
        ----------
            file_key: The full key path to the source file.
            repo: The Icechunk Repository for file appending.
        Returns
        -------
        str
            A snapshot id of the append commit.
        """
        ...

    def garbage_collect(self, repo: Repository) -> None:
        """
        Run Icechunk garbage collection and snapshot removal.

        Parameters
        ----------
            repo: And Icechunk Repository.
        Returns
        -------
            None
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
