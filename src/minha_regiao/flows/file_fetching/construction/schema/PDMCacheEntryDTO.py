from typing import Optional
from minha_regiao.flows.file_fetching.construction.schema.Schema import Schema


class PDMCacheEntryDTO(Schema):
    """Data Transfer Object for a PDM cache entry."""

    town_hall_url: str
    candidate_urls: list[str]
    best_candidate: Optional[dict] = None  # Stores PDFContentDTO data if filtered
    crawl_timestamp: Optional[str] = None
    filter_timestamp: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "candidate_urls": self.candidate_urls,
            "best_candidate": self.best_candidate,
            "crawl_timestamp": self.crawl_timestamp,
            "filter_timestamp": self.filter_timestamp,
        }
