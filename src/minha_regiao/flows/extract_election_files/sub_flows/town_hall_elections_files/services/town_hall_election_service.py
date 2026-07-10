import logging
import re
from urllib.parse import urljoin

from minha_regiao.flows.extract_election_files.schema.Election import Election, TownHallSubType

logger = logging.getLogger(__name__)

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


def filter_town_hall_file_hrefs(hrefs: list[str]) -> list[str]:
    matched = []
    for href in hrefs:
        if FILENAME_PATTERN.match(href.rsplit("/", 1)[-1]):
            matched.append(href)
        else:
            logger.debug(f"Skipping href that does not match town hall file pattern: {href}")

    logger.info(f"Filtered {len(matched)}/{len(hrefs)} hrefs as town hall election files")
    return matched


def build_town_hall_election(href: str, base_url: str) -> Election:
    basename = href.rsplit("/", 1)[-1]
    match = FILENAME_PATTERN.match(basename)
    if not match:
        logger.error(f"href does not match a town hall election file pattern: {href}")
        raise ValueError(f"href does not match a town hall election file pattern: {href}")

    election = Election(
        type="town_hall",
        sub_type=CHAMBER_SUB_TYPE_MAP[_extract_chamber(match)],
        year=_extract_year(match),
        url=urljoin(base_url, href),
        filename=basename,
    )
    logger.debug(f"Built town hall election record: {election}")
    return election
