from typing import Literal

from minha_regiao.flows.extract_election_files.schema.Schema import Schema

ElectionType = Literal[
    "presidential",
    "parliament",
    "town_hall",
    "regional",
    "referendum",
    "historical",
    "european",
]


class Election(Schema):
    type: ElectionType
    year: int
    url: str
    filename: str
