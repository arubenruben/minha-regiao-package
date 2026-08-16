from dataclasses import dataclass
from pathlib import Path

from minha_regiao.flows.extract_geo.services.SpreadsheetCodeLookup import (
    extract_ambiguous_codes_by_name,
    extract_unique_codes_by_name,
)

FREGUESIA_CODE_PATTERN = r"^\d{6}$"


@dataclass(frozen=True)
class FreguesiaSheetConfig:
    sheet_name: str
    header_row: int
    code_column: str
    name_column: str
    # National aggregate row (e.g. "TOTAL DO PAÍS"), excluded by code since its
    # code can coincidentally match FREGUESIA_CODE_PATTERN.
    national_total_code: str | None = None


# Each era's spreadsheet uses a different sheet name, header row, and column
# naming for the freguesia breakdown, even though the underlying 6-digit INE
# codes are consistent across all of them.
PRE_2013_FREGUESIA_CONFIG = FreguesiaSheetConfig(
    sheet_name="PR2006_EP_Freguesia",
    header_row=1,
    code_column="código",
    name_column="freguesia",
    national_total_code="230000",
)
POST_2013_FREGUESIA_CONFIG = FreguesiaSheetConfig(
    sheet_name="PR_2021_Freguesia",
    header_row=4,
    code_column="código",
    name_column="nome do território",
    national_total_code="500000",
)
POST_2021_FREGUESIA_CONFIG = FreguesiaSheetConfig(
    sheet_name="PR_2026_Freguesia", header_row=4, code_column="código", name_column="nome do território"
)


def _exclude_codes(config: FreguesiaSheetConfig) -> frozenset[str]:
    return frozenset({config.national_total_code}) if config.national_total_code else frozenset()


def extract_ine_codes_by_freguesia(path: Path, config: FreguesiaSheetConfig) -> dict[str, str]:
    """Maps freguesia name to INE code using the freguesia sheet of an election results spreadsheet.

    Freguesia names that are ambiguous within the spreadsheet (shared by more
    than one freguesia nationally) are left out here and handled separately
    via `extract_ambiguous_ine_codes_by_freguesia`.
    """
    return extract_unique_codes_by_name(
        path,
        config.sheet_name,
        config.header_row,
        config.code_column,
        config.name_column,
        FREGUESIA_CODE_PATTERN,
        _exclude_codes(config),
    )


def extract_ambiguous_ine_codes_by_freguesia(path: Path, config: FreguesiaSheetConfig) -> dict[str, list[str]]:
    """Maps each ambiguous freguesia name to its candidate INE codes."""
    return extract_ambiguous_codes_by_name(
        path,
        config.sheet_name,
        config.header_row,
        config.code_column,
        config.name_column,
        FREGUESIA_CODE_PATTERN,
        _exclude_codes(config),
    )
