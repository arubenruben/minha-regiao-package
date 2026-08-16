import logging
from pathlib import Path

from minha_regiao.flows.extract_geo.services.FuzzyMatch import DEFAULT_THRESHOLD, resolve_name
from minha_regiao.flows.extract_geo.services.SpreadsheetCodeLookup import (
    extract_ambiguous_codes_by_name,
    extract_unique_codes_by_name,
)

logger = logging.getLogger(__name__)

CONCELHO_SHEET_NAME = "PR_2026_Concelho"
HEADER_ROW = 4
CODE_COLUMN = "código"
NAME_COLUMN = "nome do território"
MUNICIPALITY_CODE_PATTERN = r"^\d{4}$"

# ANMP disambiguates the handful of municipality names that exist in more than
# one district by suffixing a region hint in parentheses (e.g. "Lagoa
# (Algarve)", "Lagoa (Açores)"), which the spreadsheet's plain "nome do
# território" doesn't carry. These are the region-name synonyms ANMP is known
# to use, mapped to the INE code prefix that identifies them.
REGION_HINT_CODE_PREFIXES: dict[str, str] = {
    "açores": "4",
    "madeira": "31",
    "algarve": "08",
}

# ANMP's contacts page uses the short colloquial name for these
# municipalities, while the spreadsheet uses the official INE name; neither an
# exact nor a fuzzy match bridges "Praia da Vitória" to "Vila da Praia da
# Vitória" (ratio 0.8), so it's spelled out explicitly instead of chasing it
# with a lower Levenshtein threshold, which would risk matching genuinely
# different municipalities (e.g. "Moura"/"Mora", "Miranda do Douro"/"Miranda
# do Corvo" both score above 0.85).
MUNICIPALITY_NAME_ALIASES: dict[str, str] = {
    "Praia da Vitória": "Vila da Praia da Vitória",
}


def extract_ine_codes_by_municipality(path: Path) -> dict[str, str]:
    """Maps municipality name to INE code using the concelho sheet of an election results spreadsheet.

    A handful of municipality names are ambiguous nationally (e.g. "Lagoa" and
    "Calheta" each identify two different concelhos in different districts);
    those are left out here and handled separately via
    `extract_ambiguous_ine_codes_by_municipality`.
    """
    return extract_unique_codes_by_name(
        path, CONCELHO_SHEET_NAME, HEADER_ROW, CODE_COLUMN, NAME_COLUMN, MUNICIPALITY_CODE_PATTERN
    )


def extract_ambiguous_ine_codes_by_municipality(path: Path) -> dict[str, list[str]]:
    """Maps each nationally ambiguous municipality name to its candidate INE codes."""
    return extract_ambiguous_codes_by_name(
        path, CONCELHO_SHEET_NAME, HEADER_ROW, CODE_COLUMN, NAME_COLUMN, MUNICIPALITY_CODE_PATTERN
    )


def _resolve_region_hint_code(hint: str, candidate_codes: list[str]) -> str | None:
    prefix = REGION_HINT_CODE_PREFIXES.get(hint.strip().lower())
    if prefix is None:
        return None

    matches = [code for code in candidate_codes if code.startswith(prefix)]
    return matches[0] if len(matches) == 1 else None


def _match_ambiguous_ine_code(
    name: str, ambiguous_ine_codes_by_municipality: dict[str, list[str]], threshold: float
) -> str | None:
    base_name, _, hint = name.rpartition("(")
    base_name = base_name.strip()
    hint = hint.rstrip(")").strip()

    matched_base_name = resolve_name(base_name, ambiguous_ine_codes_by_municipality, threshold)
    if matched_base_name is None:
        return None

    code = _resolve_region_hint_code(hint, ambiguous_ine_codes_by_municipality[matched_base_name])
    if code is not None:
        logger.info(f"Resolved ambiguous municipality '{name}' to INE code {code} via region hint '{hint}'")

    return code


def match_ine_code(
    name: str,
    ine_codes_by_municipality: dict[str, str],
    ambiguous_ine_codes_by_municipality: dict[str, list[str]] | None = None,
    threshold: float = DEFAULT_THRESHOLD,
) -> str | None:
    """Resolves a municipality name to its INE code.

    Tries an exact match, then the closest Levenshtein-ratio match, to absorb
    spelling drift between the ANMP contacts page and the spreadsheet's
    official names. Names carrying a region hint in parentheses (ANMP's own
    way of disambiguating e.g. "Lagoa (Algarve)" from "Lagoa (Açores)") are
    resolved against the ambiguous-name codes instead.
    """
    if ambiguous_ine_codes_by_municipality and "(" in name:
        return _match_ambiguous_ine_code(name, ambiguous_ine_codes_by_municipality, threshold)

    aliased_name = MUNICIPALITY_NAME_ALIASES.get(name)
    if aliased_name is not None:
        logger.info(f"Resolved municipality alias '{name}' to '{aliased_name}'")
        name = aliased_name

    matched_name = resolve_name(name, ine_codes_by_municipality, threshold)
    if matched_name is None:
        return None

    if matched_name != name:
        logger.info(f"Fuzzy matched '{name}' to '{matched_name}'")

    return ine_codes_by_municipality[matched_name]
