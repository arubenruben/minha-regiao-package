from typing import Optional
from minha_regiao.flows.file_fetching.construction.schema.Schema import Schema


class PDMCrawlResultDTO(Schema):
    """Data Transfer Object for the result of a PDM crawl operation."""

    town_hall_url: str
    candidate_pdf_urls: list[str]
    pages_visited: int
    crawl_timestamp: Optional[str] = None
