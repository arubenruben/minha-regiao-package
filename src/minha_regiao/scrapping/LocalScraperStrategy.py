import httpx
from typing import Optional
from bs4 import BeautifulSoup
from minha_regiao.scrapping.ScraperStrategy import ScraperStrategy


class LocalScraperStrategy(ScraperStrategy):
    def __init__(self, timeout: float = 30.0, headers: Optional[dict] = None, max_concurrency: int = 5):
        super().__init__(max_concurrency=max_concurrency)
        self.timeout = timeout
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    async def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            headers=self.headers,
            follow_redirects=True,
        ) as client:
            response = await client.get(url)
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()

        return BeautifulSoup(response.text, "html.parser")
