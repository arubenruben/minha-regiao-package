import logging
from pathlib import Path

import Levenshtein
import pandas as pd

logger = logging.getLogger(__name__)

CONCELHO_SHEET_NAME = "PR_2026_Concelho"
HEADER_ROW = 4
CODE_COLUMN = "código"
NAME_COLUMN = "nome do território"
MUNICIPALITY_CODE_PATTERN = r"^\d{4}$"
FUZZY_MATCH_THRESHOLD = 0.9


def extract_ine_codes_by_municipality(path: Path) -> dict[str, str]:
    """Maps municipality name to INE code using the concelho sheet of an election results spreadsheet.

    A handful of municipality names are ambiguous nationally (e.g. "Lagoa" and
    "Calheta" each identify two different concelhos in different districts),
    and the spreadsheet has no district column to disambiguate them, so those
    names are dropped from the result rather than mapped to an arbitrary code.
    """
    df = pd.read_excel(path, sheet_name=CONCELHO_SHEET_NAME, header=HEADER_ROW, dtype={CODE_COLUMN: str})
    df = df.dropna(subset=[CODE_COLUMN, NAME_COLUMN])
    df = df[df[CODE_COLUMN].str.match(MUNICIPALITY_CODE_PATTERN)]

    codes_by_name: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        name = str(row[NAME_COLUMN]).strip()
        code = str(row[CODE_COLUMN]).strip()
        codes_by_name.setdefault(name, []).append(code)

    ambiguous = {name: codes for name, codes in codes_by_name.items() if len(codes) > 1}
    if ambiguous:
        logger.warning(f"Skipping municipality names that map to more than one INE code: {ambiguous}")

    return {name: codes[0] for name, codes in codes_by_name.items() if len(codes) == 1}


def match_ine_code(
    name: str, ine_codes_by_municipality: dict[str, str], threshold: float = FUZZY_MATCH_THRESHOLD
) -> str | None:
    """Resolves a municipality name to its INE code, falling back to the closest
    Levenshtein-ratio match to absorb spelling drift between the ANMP contacts
    page and the spreadsheet's official names (accents, hyphenation, etc.).
    """
    exact = ine_codes_by_municipality.get(name)
    if exact is not None:
        return exact

    best_name, best_ratio = None, 0.0
    for candidate in ine_codes_by_municipality:
        ratio = Levenshtein.ratio(name, candidate)
        if ratio > best_ratio:
            best_name, best_ratio = candidate, ratio

    if best_name is None or best_ratio < threshold:
        return None

    logger.info(f"Fuzzy matched '{name}' to '{best_name}' (ratio={best_ratio:.3f})")
    return ine_codes_by_municipality[best_name]
