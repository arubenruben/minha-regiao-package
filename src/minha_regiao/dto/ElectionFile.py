import os
import requests
import pandas as pd
from pydantic import Field
from typing import Optional
from minha_regiao.dto.DTO import DTO
from tempfile import NamedTemporaryFile


class ElectionFile(DTO):
    url: str = Field(description="URL of the election file")
    filepath: str = Field(
        description="Path to the downloaded election file (always xlsx)"
    )

    hf_file_id: Optional[str] = Field(
        default=None,
        description="Hugging Face file ID after uploading the file to the HF Hub",
    )

    @classmethod
    def from_url(cls, url: str):
        """Download the file from URL, convert to xlsx if needed, and create an ElectionFile instance."""
        # Infer file format from URL
        url_lower = url.lower()
        if url_lower.endswith(".xlsx"):
            source_format = "xlsx"
        elif url_lower.endswith(".xls"):
            source_format = "xls"
        else:
            raise ValueError(
                f"Cannot infer file format from URL: {url}. URL must end with .xlsx or .xls"
            )

        # Download the file
        response = requests.get(url)

        if not response.status_code == 200:
            raise ValueError(
                f"Failed to download file from {url}. Status code: {response.status_code}"
            )

        # Save downloaded file with original format
        with NamedTemporaryFile(delete=False, suffix=f".{source_format}") as tmp_file:
            tmp_file.write(response.content)
            temp_filepath = tmp_file.name

        # Convert to xlsx if necessary
        if source_format == "xls":
            df = pd.read_excel(temp_filepath, engine="xlrd")
            with NamedTemporaryFile(delete=False, suffix=".xlsx") as xlsx_file:
                xlsx_filepath = xlsx_file.name
            df.to_excel(xlsx_filepath, index=False, engine="openpyxl")
            # Clean up the temporary xls file
            os.remove(temp_filepath)
            filepath = xlsx_filepath
        else:
            filepath = temp_filepath

        return cls(
            url=url,
            filepath=filepath,
        )

    def get_df(self):
        return pd.read_excel(self.filepath)
