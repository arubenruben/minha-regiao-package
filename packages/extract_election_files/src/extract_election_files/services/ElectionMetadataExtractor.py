import logging

from extract_election_files.prompt.ElectionMetadataPrompt import ElectionMetadataPrompt
from extract_election_files.schema.Election import Election
from extract_election_files.schema.ElectionMetadata import ElectionMetadata
from minha_regiao.llm.StructuredOutputStrategy import StructuredOutputStrategy

logger = logging.getLogger(__name__)


class ElectionMetadataExtractor:
    """Structures the free-text parts of Election records (name, presidential round) via an LLM,
    issuing one request per election, run concurrently."""

    def __init__(self, strategy: StructuredOutputStrategy):
        self._strategy = strategy

    async def extract(self, elections: list[Election]) -> list[ElectionMetadata]:
        if not elections:
            return []

        logger.info(f"Requesting structured metadata for {len(elections)} elections")

        prompts = [ElectionMetadataPrompt.build(election) for election in elections]
        return await self._strategy.generate_batch(prompts, ElectionMetadata)
