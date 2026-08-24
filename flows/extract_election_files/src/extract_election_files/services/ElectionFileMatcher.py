import logging
import re
from typing import Callable
from urllib.parse import urljoin

from extract_election_files.schema.Election import Election, ElectionType, SubType

logger = logging.getLogger(__name__)

YearExtractor = Callable[[re.Match], int]
SubTypeExtractor = Callable[[re.Match], SubType]
BasenameNormalizer = Callable[[str], str]

_IDENTITY: BasenameNormalizer = lambda basename: basename  # noqa: E731


class ElectionFileMatcher:
    """Matches election result file hrefs against a filename pattern and builds Election records from them."""

    def __init__(
        self,
        domain: str,
        pattern: re.Pattern,
        election_type: ElectionType,
        year_extractor: YearExtractor,
        sub_type_extractor: SubTypeExtractor | None = None,
        normalize_basename: BasenameNormalizer = _IDENTITY,
    ):
        self._domain = domain
        self._pattern = pattern
        self._election_type = election_type
        self._year_extractor = year_extractor
        self._sub_type_extractor = sub_type_extractor
        self._normalize_basename = normalize_basename

    def filter_hrefs(self, hrefs: list[str]) -> list[str]:
        matched = []
        for href in hrefs:
            if self._pattern.match(self._basename(href)):
                matched.append(href)
            else:
                logger.debug(f"Skipping href that does not match {self._domain} file pattern: {href}")

        logger.info(f"Filtered {len(matched)}/{len(hrefs)} hrefs as {self._domain} election files")
        return matched

    def build_election(self, href: str, base_url: str) -> Election:
        basename = self._basename(href)
        match = self._pattern.match(basename)
        if not match:
            logger.error(f"href does not match a {self._domain} election file pattern: {href}")
            raise ValueError(f"href does not match a {self._domain} election file pattern: {href}")

        election = Election(
            type=self._election_type,
            sub_type=self._sub_type_extractor(match) if self._sub_type_extractor else None,
            year=self._year_extractor(match),
            url=urljoin(base_url, href),
            filename=basename,
        )
        logger.debug(f"Built {self._domain} election record: {election}")
        return election

    def _basename(self, href: str) -> str:
        return self._normalize_basename(href.rsplit("/", 1)[-1])
