from typing import Optional
from minha_regiao.flows.file_fetching.construction.schema.Schema import Schema
from minha_regiao.flows.file_fetching.construction.schema.PDMCandidateDTO import PDMCandidateDTO
from minha_regiao.flows.file_fetching.construction.schema.PDMCrawlResultDTO import PDMCrawlResultDTO


class PDMFetchResultDTO(Schema):
    """Data Transfer Object for the complete PDM fetch operation result."""

    town_halls_processed: int
    total_candidates_found: int
    candidates: list[PDMCandidateDTO]
    crawl_results: list[PDMCrawlResultDTO] = []
