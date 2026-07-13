from minha_regiao.flows.extract_election_files.schema.Election import Election
from minha_regiao.flows.extract_election_files.schema.ElectionMetadata import ElectionMetadata


def extract_election_metadata(election: Election) -> ElectionMetadata:
    """Structures the parts of an Election that aren't derivable from its filename/url via regex (name, presidential round).

    Stub — not yet wired to an LLM/instructor client.
    """
    raise NotImplementedError("LLM-backed election metadata extraction is not implemented yet")
