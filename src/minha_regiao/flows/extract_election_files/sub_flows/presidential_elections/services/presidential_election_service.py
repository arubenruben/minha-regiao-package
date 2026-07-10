import logging
import re
from urllib.parse import urljoin

from minha_regiao.flows.extract_election_files.schema.Election import Election

logger = logging.getLogger(__name__)

FILENAME_PATTERN = re.compile(r"^PR[_ ]?(\d{4}).*\.xlsx?$", re.IGNORECASE)


def filter_presidential_file_hrefs(hrefs: list[str]) -> list[str]:
    matched = []
    for href in hrefs:
        if FILENAME_PATTERN.match(href.rsplit("/", 1)[-1]):
            matched.append(href)
        else:
            logger.debug(f"Skipping href that does not match presidential file pattern: {href}")

    logger.info(f"Filtered {len(matched)}/{len(hrefs)} hrefs as presidential election files")
    return matched


def build_presidential_election(href: str, base_url: str) -> Election:
    basename = href.rsplit("/", 1)[-1]
    match = FILENAME_PATTERN.match(basename)
    if not match:
        logger.error(f"href does not match a presidential election file pattern: {href}")
        raise ValueError(f"href does not match a presidential election file pattern: {href}")

    election = Election(
        type="presidential",
        year=int(match.group(1)),
        url=urljoin(base_url, href),
        filename=basename,
    )
    logger.debug(f"Built presidential election record: {election}")
    return election
