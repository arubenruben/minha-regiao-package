import logging

from minha_regiao.flows.extract_election_files.prompt.ElectionMetadataPrompt import ElectionMetadataPrompt
from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.schema.ElectionMetadata import ElectionMetadata
from minha_regiao.llm.GeminiStructuredClient import GeminiStructuredClient

logger = logging.getLogger(__name__)


class ElectionMetadataExtractor:
    """Structures the free-text parts of Election records (name, presidential round) with Gemini,
    issuing one request per election, run concurrently."""

    def __init__(self, api_key: str, model: str):
        self._client = GeminiStructuredClient(api_key, model)

    async def extract(self, elections: list[Election]) -> list[ElectionMetadata]:
        if not elections:
            return []

        logger.info(f"Requesting structured metadata for {len(elections)} elections")

        prompts = [ElectionMetadataPrompt.build(election) for election in elections]
        return await self._client.generate_batch(prompts, ElectionMetadata)
