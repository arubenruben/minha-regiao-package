from typing import Optional
from minha_regiao.flows.file_fetching.construction.schema.Schema import Schema


class PDMCacheEntryDTO(Schema):
    """Data Transfer Object for a PDM cache entry."""

    town_hall_url: str
    candidate_urls: list[str]
    last_updated: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            self.town_hall_url: self.candidate_urls,
        }
