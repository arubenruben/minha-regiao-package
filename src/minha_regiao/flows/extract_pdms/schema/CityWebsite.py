from pydantic import BaseModel


class CityWebsite(BaseModel):
    id: int
    name: str
    website: str
