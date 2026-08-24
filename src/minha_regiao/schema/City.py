from minha_regiao.schema.Schema import Schema
from minha_regiao.schema.TownHall import TownHall

class City(Schema):
    name: str
    district: str
    town_hall: TownHall