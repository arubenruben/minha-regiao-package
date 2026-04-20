from abc import ABC, abstractmethod
from bs4 import BeautifulSoup

class ScraperStrategy(ABC):
    """Abstract base class for scraping strategies."""
    
    @abstractmethod
    async def query(self, url: str) -> BeautifulSoup:
        """Fetch a URL and return the parsed BeautifulSoup object."""
        pass
