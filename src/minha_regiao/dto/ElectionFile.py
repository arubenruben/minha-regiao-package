import pandas as pd
from typing import Optional
from minha_regiao.dto.DTO import DTO
from tempfile import NamedTemporaryFile
from pydantic import model_validator, Field

class ElectionFile(DTO):
    url: str
    year: int
    election_type: str  # e.g., 'presidential', 'municipal', 'parliamentary'
    filepath: Optional[str] = Field(
        default=None, description="Path to the downloaded election file"
    )
    
    def get_df(self):
        if not self.filepath:
            raise ValueError("Filepath is not set. Please download the file first.")
        return pd.read_excel(self.filepath)

    @model_validator(mode="after")
    def download_file(self):
        raise NotImplementedError("File download logic not implemented yet")
