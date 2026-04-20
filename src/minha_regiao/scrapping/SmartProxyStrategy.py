import httpx
from typing import Optional
from bs4 import BeautifulSoup
from minha_regiao.scrapping.ScraperStrategy import ScraperStrategy


class SmartProxyStrategy(ScraperStrategy):
    def __init__(
        self,
        api_key: str,
        geo: str = "PT",
        locale: str = "pt-PT",
        js_render: bool = True,
        format_list: list | None = None,
        screenshot_type: int = 1,
        source: str = "uni_scraper",
    ):
        self.api_key = api_key
        self.geo = geo
        self.locale = locale
        self.js_render = js_render
        self.format_list = format_list or ["html"]
        self.screenshot_type = screenshot_type
        self.source = source

    async def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        headers = {
            "Authorization": f"Basic {self.api_key}",
            "Content-Type": "application/json",
        }

        parameters = {
            "geo": self.geo,
            "locale": self.locale,
            "js_render": self.js_render,
            "format_list": self.format_list,
            "screenshot_type": self.screenshot_type,
            "source": self.source,
            "context": {"url": url, "screenshot_type": self.screenshot_type},
        }

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(300.0),
        ) as client:
            response = await client.post(
                "https://scraper.smartproxy.org/v1/query",
                headers=headers,
                json=parameters,
            )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        return BeautifulSoup(response.text, "html.parser")
