from pydantic import BaseModel, ConfigDict

from minha_regiao.entity.City import City


class ParishCityLink(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    ine_code: str
    city: City
