from typing import List
from pydantic import Field
from abc import ABC, abstractmethod
from minha_regiao.dto.DTO import DTO
from minha_regiao.dto.ElectionFile import ElectionFile


class Election(DTO, ABC):
    year: int
    url: str = Field(description="URL of the election data source")

    @abstractmethod
    def _prepare_election_files(self) -> List[ElectionFile]:
        """Subclasses must implement this method to set hf_file_id for their files."""
        raise NotImplementedError("Subclasses must implement this method.")

    def get_election_files(self) -> List[ElectionFile]:
        """Template method that prepares files and validates all have hf_file_id set."""
        files = self._prepare_election_files()
        
        # Validate that all files have hf_file_id set
        for file in files:
            if file.hf_file_id is None:
                raise ValueError(
                    f"hf_file_id must be set for all election files. "
                    f"Missing for file in {self.__class__.__name__}"
                )
        
        return files