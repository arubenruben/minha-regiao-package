from typing import Literal, get_args

from pydantic import model_validator

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

TownHallSubType = Literal[
    "council",  # CM - Câmara Municipal
    "assembly",  # AM - Assembleia Municipal
    "parish_assembly",  # AF - Assembleia de Freguesia
]

RegionalSubType = Literal[
    "madeira",  # ALRAM - Assembleia Legislativa Regional da Madeira
    "azores",  # ALRAA - Assembleia Legislativa Regional dos Açores
]

SubType = TownHallSubType | RegionalSubType

# Election types that require a sub_type, mapped to the sub_type values they accept.
SUB_TYPES_BY_ELECTION_TYPE: dict[ElectionType, tuple[str, ...]] = {
    "town_hall": get_args(TownHallSubType),
    "regional": get_args(RegionalSubType),
}


class Election(Schema):
    type: ElectionType
    sub_type: SubType | None = None
    year: int
    url: str
    filename: str

    @model_validator(mode="after")
    def _validate_sub_type(self) -> "Election":
        allowed_sub_types = SUB_TYPES_BY_ELECTION_TYPE.get(self.type)

        if allowed_sub_types is None:
            if self.sub_type is not None:
                raise ValueError(f"sub_type must be None for type '{self.type}'")
        elif self.sub_type not in allowed_sub_types:
            raise ValueError(f"sub_type is required for type '{self.type}' and must be one of {allowed_sub_types}")

        return self
