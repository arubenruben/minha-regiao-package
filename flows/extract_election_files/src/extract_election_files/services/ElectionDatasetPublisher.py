import logging

from datasets import Dataset

from extract_election_files.schema.StructuredElection import StructuredElection

logger = logging.getLogger(__name__)


class ElectionDatasetPublisher:
    """Builds a Hugging Face `datasets.Dataset` from structured election records and pushes it to the Hub."""

    def __init__(self, repo_id: str, token: str, config_name: str):
        self._repo_id = repo_id
        self._token = token
        self._config_name = config_name

    def publish(self, elections: list[StructuredElection]) -> None:
        dataset = Dataset.from_list([election.model_dump() for election in elections])

        logger.info(f"Pushing {len(dataset)} election records to {self._repo_id} (config: {self._config_name})")
        dataset.push_to_hub(self._repo_id, config_name=self._config_name, token=self._token)
