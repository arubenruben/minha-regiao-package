from pydantic import BaseModel


class CityDatasetRecord(BaseModel):
    city_name: str
    ine_code: str
    district_name: str | None
    town_hall_email: str | None
    town_hall_website: str | None
