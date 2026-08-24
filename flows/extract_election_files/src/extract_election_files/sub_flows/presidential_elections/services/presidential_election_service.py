import re

from extract_election_files.services.ElectionFileMatcher import ElectionFileMatcher

FILENAME_PATTERN = re.compile(r"^PR[_ ]?(\d{4}).*\.xlsx?$", re.IGNORECASE)

presidential_election_matcher = ElectionFileMatcher(
    domain="presidential",
    pattern=FILENAME_PATTERN,
    election_type="presidential",
    year_extractor=lambda match: int(match.group(1)),
)
