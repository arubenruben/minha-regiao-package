import logging

from datasets import Dataset

from minha_regiao.flows.extract_districts.schema.DistrictDatasetRecord import DistrictDatasetRecord

logger = logging.getLogger(__name__)


class DistrictDatasetPublisher:
    """Builds a Hugging Face `datasets.Dataset` from district records and pushes it to the Hub."""

    def __init__(self, repo_id: str, token: str, config_name: str):
        self._repo_id = repo_id
        self._token = token
        self._config_name = config_name

    def publish(self, records: list[DistrictDatasetRecord]) -> None:
        dataset = Dataset.from_list([record.model_dump() for record in records])

        logger.info(f"Pushing {len(dataset)} district records to {self._repo_id} (config: {self._config_name})")
        dataset.push_to_hub(self._repo_id, config_name=self._config_name, token=self._token)
