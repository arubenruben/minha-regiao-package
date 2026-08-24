from enum import Enum

from tortoise import fields

from minha_regiao.entity.elections.Election import Election


class TownHallSubType(str, Enum):
    COUNCIL = "council"  # CM - Câmara Municipal
    ASSEMBLY = "assembly"  # AM - Assembleia Municipal
    PARISH_ASSEMBLY = "parish_assembly"  # AF - Assembleia de Freguesia


class TownHallElection(Election):
    sub_type = fields.CharEnumField(TownHallSubType)

    class Meta:
        table = "town_hall_election"
