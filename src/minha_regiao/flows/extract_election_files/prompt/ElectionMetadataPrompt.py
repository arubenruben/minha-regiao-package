from langchain_core.prompts import ChatPromptTemplate

from minha_regiao.flows.extract_election_files.schema.Election import Election


class ElectionMetadataPrompt:
    """Builds the prompt used to structure an Election's free-text metadata (name, presidential round) via an LLM."""

    _TEMPLATE = ChatPromptTemplate.from_template(
        """You are structuring metadata for a Portuguese election result file scraped \
from the Ministério da Administração Interna (MAI) website.

Given this election:
- type: {type}
- sub_type: {sub_type}
- year: {year}
- filename: {filename}

Infer:
- name: a human-readable Portuguese election name, e.g. "Eleições Legislativas 2024", \
"Eleição Presidencial 2026 - 2ª Volta".
- round: only if type == "presidential", either "first_round" or "second_round" — infer this \
from filename/url hints (e.g. "Sufrágio", "Volta", "2º"). Leave it unset for every other type.
"""
    )

    @staticmethod
    def build(election: Election) -> str:
        message = ElectionMetadataPrompt._TEMPLATE.format_messages(
            type=election.type,
            sub_type=election.sub_type,
            year=election.year,
            filename=election.filename,
        )[0]
        return message.content
