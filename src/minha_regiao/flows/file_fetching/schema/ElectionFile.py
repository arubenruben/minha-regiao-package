from typing import Optional
from pydantic import Field
from minha_regiao.flows.file_fetching.schema.Schema import Schema
from minha_regiao.flows.file_fetching.schema.Election import Election

class ElectionFile(Schema):
    election: Election
    file_url: str
    hf_file_url: Optional[str] = Field(None, description="URL of the file in the Hugging Face repository after upload", exclude=True)