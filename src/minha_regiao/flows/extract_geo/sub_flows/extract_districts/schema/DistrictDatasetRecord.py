from pydantic import BaseModel


class DistrictDatasetRecord(BaseModel):
    name: str
    ine_prefix: str
    wikipedia_url: str | None
