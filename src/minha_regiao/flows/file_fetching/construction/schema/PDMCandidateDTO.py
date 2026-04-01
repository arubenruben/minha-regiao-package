from typing import Optional
from minha_regiao.flows.file_fetching.construction.schema.Schema import Schema


class PDMCandidateDTO(Schema):
    """Data Transfer Object for a PDM (Plano Diretor Municipal) candidate URL."""

    url: str
    source_town_hall: str
    discovered_at: Optional[str] = None
    metadata: Optional[dict] = None
