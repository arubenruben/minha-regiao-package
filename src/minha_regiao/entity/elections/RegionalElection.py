from enum import Enum

from tortoise import fields

from minha_regiao.entity.elections.Election import Election


class RegionalSubType(str, Enum):
    MADEIRA = "madeira"  # ALRAM - Assembleia Legislativa Regional da Madeira
    AZORES = "azores"  # ALRAA - Assembleia Legislativa Regional dos Açores


class RegionalElection(Election):
    sub_type = fields.CharEnumField(RegionalSubType)

    class Meta:
        table = "regional_election"
