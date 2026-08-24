from pydantic import BaseModel


class DistrictReference(BaseModel):
    name: str
    ine_prefix: str
