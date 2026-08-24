from typing import Literal

from extract_election_files.schema.Schema import Schema

PresidentialRound = Literal["first_round", "second_round"]


class ElectionMetadata(Schema):
    """Free-text metadata about an Election that can't be parsed deterministically from its filename/url and is instead structured by an LLM."""

    name: str
    round: PresidentialRound | None = None
