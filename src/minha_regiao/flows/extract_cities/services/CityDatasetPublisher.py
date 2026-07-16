import logging

from datasets import Dataset

from minha_regiao.flows.extract_cities.schema.CityDatasetRecord import CityDatasetRecord

logger = logging.getLogger(__name__)


class CityDatasetPublisher:
    """Builds a Hugging Face `datasets.Dataset` from city contact records and pushes it to the Hub."""

    def __init__(self, repo_id: str, token: str):
        self._repo_id = repo_id
        self._token = token

    def publish(self, records: list[CityDatasetRecord]) -> None:
        dataset = Dataset.from_list([record.model_dump() for record in records])

        logger.info(f"Pushing {len(dataset)} city records to {self._repo_id}")
        dataset.push_to_hub(self._repo_id, token=self._token)
