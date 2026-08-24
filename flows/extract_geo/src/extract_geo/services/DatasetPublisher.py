import logging
from typing import Generic, TypeVar

from datasets import Dataset
from pydantic import BaseModel

logger = logging.getLogger(__name__)

RecordT = TypeVar("RecordT", bound=BaseModel)


class DatasetPublisher(Generic[RecordT]):
    """Builds a Hugging Face `datasets.Dataset` from pydantic records and pushes it to the Hub."""

    def __init__(self, repo_id: str, token: str | None, config_name: str):
        self._repo_id = repo_id
        self._token = token
        self._config_name = config_name

    def publish(self, records: list[RecordT]) -> None:
        dataset = Dataset.from_list([record.model_dump() for record in records])

        logger.info(f"Pushing {len(dataset)} records to {self._repo_id} (config: {self._config_name})")
        dataset.push_to_hub(self._repo_id, config_name=self._config_name, token=self._token)
