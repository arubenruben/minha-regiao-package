from pydantic import BaseModel


class PDMResult(BaseModel):
    city_id: int
    source_url: str
    pdf_url: str
