from typing import Optional
from minha_regiao.flows.file_fetching.construction.schema.Schema import Schema


class TownHallDTO(Schema):
    """Data Transfer Object for a Town Hall (Câmara Municipal)."""

    url: str
    name: Optional[str] = None
    domain: Optional[str] = None

    @property
    def municipality_name(self) -> Optional[str]:
        """Extract municipality name from URL (e.g., 'cm-maia' -> 'maia')."""
        if self.name:
            return self.name
        if self.domain:
            parts = self.domain.replace("www.cm-", "").replace("www.", "").rstrip(".pt")
            return parts
        return None
