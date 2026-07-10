import logging
import re
from urllib.parse import unquote, urljoin

from minha_regiao.flows.extract_election_files.schema.Election import Election, RegionalSubType

logger = logging.getLogger(__name__)

# ALRAM = Assembleia Legislativa Regional da Madeira
# ALRAA = Assembleia Legislativa Regional dos Açores
FILENAME_PATTERN = re.compile(
    r"^resultados[ _]*alra(?P<region>m|a)(?P<year>\d{4})\.xlsx?$",
    re.IGNORECASE,
)

REGION_SUB_TYPE_MAP: dict[str, RegionalSubType] = {
    "m": "madeira",
    "a": "azores",
}


def filter_regional_file_hrefs(hrefs: list[str]) -> list[str]:
    matched = []
    for href in hrefs:
        basename = unquote(href.rsplit("/", 1)[-1])
        if FILENAME_PATTERN.match(basename):
            matched.append(href)
        else:
            logger.debug(f"Skipping href that does not match regional file pattern: {href}")

    logger.info(f"Filtered {len(matched)}/{len(hrefs)} hrefs as regional election files")
    return matched


def build_regional_election(href: str, base_url: str) -> Election:
    basename = unquote(href.rsplit("/", 1)[-1])
    match = FILENAME_PATTERN.match(basename)
    if not match:
        logger.error(f"href does not match a regional election file pattern: {href}")
        raise ValueError(f"href does not match a regional election file pattern: {href}")

    election = Election(
        type="regional",
        sub_type=REGION_SUB_TYPE_MAP[match.group("region").lower()],
        year=int(match.group("year")),
        url=urljoin(base_url, href),
        filename=basename,
    )
    logger.debug(f"Built regional election record: {election}")
    return election
