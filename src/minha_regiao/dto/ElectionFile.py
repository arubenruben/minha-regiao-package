import requests
import pandas as pd
from typing import Literal
from pydantic import Field
from minha_regiao.dto.DTO import DTO
from tempfile import NamedTemporaryFile


class ElectionFile(DTO):
    url: str
    year: int
    election_type: str  # e.g., 'presidential', 'municipal', 'parliamentary'
    filepath: str = Field(description="Path to the downloaded election file")
    file_format: Literal["xlsx", "xls"] = Field(
        description="Format of the election file"
    )

    @classmethod
    def from_url(
        cls,
        url: str,
        year: int,
        election_type: str,
        file_format: Literal["xlsx", "xls"],
    ):
        """Download the file from URL and create an ElectionFile instance."""
        with NamedTemporaryFile(delete=False, suffix=f".{file_format}") as tmp_file:
            response = requests.get(url)

            if not response.status_code == 200:
                raise ValueError(
                    f"Failed to download file from {url}. Status code: {response.status_code}"
                )

            tmp_file.write(response.content)

            filepath = tmp_file.name

        return cls(
            url=url,
            year=year,
            election_type=election_type,
            filepath=filepath,
            file_format=file_format,
        )

    def get_df(self):
        return pd.read_excel(self.filepath)
