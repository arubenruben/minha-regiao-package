import httpx
from bs4 import BeautifulSoup
from minha_regiao.scrapping.Scraper import ScraperStrategy

class LocalScraper(ScraperStrategy):
    """Scraper strategy that uses local network connection."""
    
    def __init__(self, timeout: float = 30.0, headers: dict = None):
        self.timeout = timeout
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    async def query(self, url: str) -> BeautifulSoup:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            headers=self.headers,
            follow_redirects=True
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            
        return BeautifulSoup(response.text, "html.parser")
