from pydantic import BaseModel


class CityWebsite(BaseModel):
    id: int
    website: str
