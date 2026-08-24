import json
from pathlib import Path

from extract_geo.schema.DistrictReference import DistrictReference

DEFAULT_DATA_PATH = Path(__file__).parents[1] / "data" / "districts.json"


def load_district_references(path: Path = DEFAULT_DATA_PATH) -> list[DistrictReference]:
    references = json.loads(path.read_text(encoding="utf-8"))
    return [DistrictReference(**reference) for reference in references]


def match_district_name(ine_code: str, references: list[DistrictReference]) -> str | None:
    """Resolves an INE code to its district name via the longest matching prefix.

    Mainland districts key on the full 2-digit district code (e.g. "13" for
    Porto). Madeira and Açores are matched on a single leading digit ("3"/"4")
    instead, since their concelhos are split across several 2-digit INE
    codes (31/32 for Madeira, 41-49 for Açores) rather than one.
    """
    matches = [reference for reference in references if ine_code.startswith(reference.ine_prefix)]
    if not matches:
        return None

    return max(matches, key=lambda reference: len(reference.ine_prefix)).name
