import re

from extract_election_files.schema.Election import TownHallSubType
from extract_election_files.services.ElectionFileMatcher import ElectionFileMatcher

# resultados_eleicoes_AUT25_CM.xlsx (2025, 2-digit year embedded in "AUT25")
# resultados_eleicoes_CM_2021.xlsx  (2021 and earlier, chamber before the year)
# CM = Câmara Municipal, AM = Assembleia Municipal, AF = Assembleia de Freguesia
FILENAME_PATTERN = re.compile(
    r"^resultados_eleicoes_(?:aut(?P<year2>\d{2})_(?P<chamber1>cm|am|af)|(?P<chamber2>cm|am|af)_(?P<year4>\d{4}))\.xlsx?$",
    re.IGNORECASE,
)

CHAMBER_SUB_TYPE_MAP: dict[str, TownHallSubType] = {
    "cm": "council",
    "am": "assembly",
    "af": "parish_assembly",
}


def _extract_year(match: re.Match) -> int:
    if match.group("year4"):
        return int(match.group("year4"))
    return 2000 + int(match.group("year2"))


def _extract_chamber(match: re.Match) -> str:
    return (match.group("chamber1") or match.group("chamber2")).lower()


town_hall_election_matcher = ElectionFileMatcher(
    domain="town_hall",
    pattern=FILENAME_PATTERN,
    election_type="town_hall",
    year_extractor=_extract_year,
    sub_type_extractor=lambda match: CHAMBER_SUB_TYPE_MAP[_extract_chamber(match)],
)
