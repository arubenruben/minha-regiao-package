from pydantic import model_validator

from extract_election_files.schema.Election import Election, ElectionType, SubType
from extract_election_files.schema.ElectionMetadata import ElectionMetadata, PresidentialRound
from extract_election_files.schema.Schema import Schema


class StructuredElection(Schema):
    type: ElectionType
    sub_type: SubType | None = None
    year: int
    url: str
    raw_file_url: str | None = None
    filename: str
    name: str
    round: PresidentialRound | None = None

    @model_validator(mode="after")
    def _validate_round(self) -> "StructuredElection":
        if self.type == "presidential":
            if self.round is None:
                raise ValueError("round is required for type 'presidential'")
        else:
            self.round = None

        return self

    @classmethod
    def from_election(cls, election: Election, metadata: ElectionMetadata) -> "StructuredElection":
        return cls(
            type=election.type,
            sub_type=election.sub_type,
            year=election.year,
            url=election.url,
            filename=election.filename,
            name=metadata.name,
            round=metadata.round,
        )
