from typing import Optional
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod


class ScraperStrategy(ABC):
    @abstractmethod
    async def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        """Internal method to fetch a URL and return the parsed BeautifulSoup object.

        Must be implemented by subclasses.
        """
        pass

    async def query(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a URL and return the parsed BeautifulSoup object.
        
        Concurrency is managed by the calling code (e.g., workers in flows).
        """
        return await self._fetch(url)
