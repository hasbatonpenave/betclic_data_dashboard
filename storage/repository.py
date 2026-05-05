"""
storage/repository.py — abstract storage interface.

Swap SQLiteRepository for any other backend without touching server/ or feed/.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from api.models import OddsUpdate, PricePoint


class PriceRepository(ABC):

    @abstractmethod
    def enqueue(self, update: OddsUpdate) -> None:
        """
        Non-blocking enqueue of a price update for persistence.
        Called from the async event loop — must never block.
        """

    @abstractmethod
    def get_history(
        self,
        match_id:  str,
        selection: str,
        market:    str,
        limit:     int,
    ) -> list[PricePoint]:
        """
        Synchronous read — always called via run_in_executor.
        """

    @abstractmethod
    def start(self) -> None:
        """Start the background writer thread."""

    @abstractmethod
    def stop(self) -> None:
        """Signal shutdown and flush remaining rows."""

    @abstractmethod
    def join(self, timeout: float = 10.0) -> None:
        """Block until the writer thread has flushed and exited."""
