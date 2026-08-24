import re

from extract_election_files.services.ElectionFileMatcher import ElectionFileMatcher

# AR = Assembleia da República; AC = Assembleia Constituinte (1975), listed
# on the same AssembleiaRepublica page and treated as a parliament election.
FILENAME_PATTERN = re.compile(r"^(AR|AC)[_ ]?(\d{4}).*\.xlsx?$", re.IGNORECASE)

parliament_election_matcher = ElectionFileMatcher(
    domain="parliament",
    pattern=FILENAME_PATTERN,
    election_type="parliament",
    year_extractor=lambda match: int(match.group(2)),
)
