import re

from minha_regiao.flows.extract_election_files.services.ElectionFileMatcher import ElectionFileMatcher

FILENAME_PATTERN = re.compile(r"^PE(\d{4}).*\.xlsx?$", re.IGNORECASE)

european_election_matcher = ElectionFileMatcher(
    domain="european",
    pattern=FILENAME_PATTERN,
    election_type="european",
    year_extractor=lambda match: int(match.group(1)),
)
