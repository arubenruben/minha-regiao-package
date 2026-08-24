import httpx
from bs4 import BeautifulSoup
from minha_regiao.scrapping.ScraperStrategy import ScraperStrategy


class DecodoProxyStrategy(ScraperStrategy):
    def __init__(
        self,
        api_key: str,
        geo: str = "Portugal",
        locale: str = "pt-pt",
        headless: str = "html",
        proxy_pool: str = "standard",
        device_type: str = "desktop_chrome",
    ):
        self.api_key = api_key
        self.geo = geo
        self.locale = locale
        self.headless = headless
        self.proxy_pool = proxy_pool
        self.device_type = device_type

    async def _fetch(self, url: str) -> BeautifulSoup:
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Basic {self.api_key}",
        }

        payload = {
            "url": url,
            "proxy_pool": self.proxy_pool,
            "headless": self.headless,
            "geo": self.geo,
            "locale": self.locale,
            "device_type": self.device_type,
        }

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(300.0),
        ) as client:
            response = await client.post(
                "https://scraper-api.decodo.com/v2/scrape",
                headers=headers,
                json=payload,
            )

        response.raise_for_status()

        return BeautifulSoup(response.text, "html.parser")
