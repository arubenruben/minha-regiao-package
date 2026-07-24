from pydantic import BaseModel


class ParishDatasetRecord(BaseModel):
    parish_name: str
    ine_code: str
    era: str
    city_name: str
    city_ine_code: str
    wikipedia_url: str | None = None
