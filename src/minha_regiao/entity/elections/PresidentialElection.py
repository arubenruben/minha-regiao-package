from enum import Enum

from tortoise import fields

from minha_regiao.entity.elections.Election import Election


class PresidentialRound(str, Enum):
    FIRST_ROUND = "first_round"
    SECOND_ROUND = "second_round"


class PresidentialElection(Election):
    round = fields.CharEnumField(PresidentialRound)

    class Meta:
        table = "presidential_election"
