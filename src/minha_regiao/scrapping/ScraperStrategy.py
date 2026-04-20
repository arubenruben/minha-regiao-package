import asyncio
from typing import Optional
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod


class ScraperStrategy(ABC):
    def __init__(self, max_concurrency: int = 5):
        """Initialize scraper with concurrency control.

        Args:
            max_concurrency: Maximum number of concurrent requests allowed
        """
        self._semaphore = asyncio.Semaphore(max_concurrency)

    @abstractmethod
    async def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        """Internal method to fetch a URL and return the parsed BeautifulSoup object.

        Must be implemented by subclasses.
        """
        pass

    async def query(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a URL with concurrency control.

        This method wraps _fetch with semaphore-based concurrency control.
        """
        async with self._semaphore:
            return await self._fetch(url)
