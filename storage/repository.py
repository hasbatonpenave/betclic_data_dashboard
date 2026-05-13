"""
storage/repository.py — Abstract base class for price storage.
"""

from abc import ABC, abstractmethod


class PriceRepository(ABC):

    @abstractmethod
    def enqueue(self, row: tuple) -> None:
        """Non-blocking enqueue of a price row for batch insert."""

    @abstractmethod
    def start(self) -> None:
        """Start the background writer thread."""

    @abstractmethod
    def stop(self) -> None:
        """Signal the writer to flush and exit."""

    @abstractmethod
    def join(self, timeout: float = 10.0) -> None:
        """Wait for the writer thread to finish."""
