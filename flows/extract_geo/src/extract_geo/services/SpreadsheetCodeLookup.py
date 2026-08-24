import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def group_codes_by_name(
    path: Path,
    sheet_name: str,
    header_row: int,
    code_column: str,
    name_column: str,
    code_pattern: str,
    exclude_codes: frozenset[str] = frozenset(),
) -> dict[str, list[str]]:
    """Groups territory codes by name from a sheet of an election results spreadsheet.

    `exclude_codes` drops rows by code rather than name, since national/regional
    aggregate rows (e.g. "TOTAL DO PAÍS") can otherwise coincidentally match
    `code_pattern` and get mistaken for a real territory.
    """
    df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, dtype={code_column: str})
    df = df.dropna(subset=[code_column, name_column])
    df = df[df[code_column].str.match(code_pattern) & ~df[code_column].isin(exclude_codes)]

    codes_by_name: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        name = str(row[name_column]).strip()
        code = str(row[code_column]).strip()
        codes_by_name.setdefault(name, []).append(code)

    return codes_by_name


def extract_unique_codes_by_name(
    path: Path,
    sheet_name: str,
    header_row: int,
    code_column: str,
    name_column: str,
    code_pattern: str,
    exclude_codes: frozenset[str] = frozenset(),
) -> dict[str, str]:
    """Maps each unambiguous territory name to its code.

    Names that map to more than one code (e.g. a name shared by territories in
    different districts) are left out here; use `extract_ambiguous_codes_by_name`
    to handle those separately.
    """
    codes_by_name = group_codes_by_name(
        path, sheet_name, header_row, code_column, name_column, code_pattern, exclude_codes
    )

    ambiguous = {name: codes for name, codes in codes_by_name.items() if len(codes) > 1}
    if ambiguous:
        logger.warning(f"Territory names in '{sheet_name}' that map to more than one code: {ambiguous}")

    return {name: codes[0] for name, codes in codes_by_name.items() if len(codes) == 1}


def extract_ambiguous_codes_by_name(
    path: Path,
    sheet_name: str,
    header_row: int,
    code_column: str,
    name_column: str,
    code_pattern: str,
    exclude_codes: frozenset[str] = frozenset(),
) -> dict[str, list[str]]:
    """Maps each nationally ambiguous territory name to its candidate codes."""
    codes_by_name = group_codes_by_name(
        path, sheet_name, header_row, code_column, name_column, code_pattern, exclude_codes
    )
    return {name: codes for name, codes in codes_by_name.items() if len(codes) > 1}
