import re
from urllib.parse import unquote

from minha_regiao.flows.extract_election_files.schema.Election import RegionalSubType
from minha_regiao.flows.extract_election_files.services.ElectionFileMatcher import ElectionFileMatcher

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

regional_election_matcher = ElectionFileMatcher(
    domain="regional",
    pattern=FILENAME_PATTERN,
    election_type="regional",
    year_extractor=lambda match: int(match.group("year")),
    sub_type_extractor=lambda match: REGION_SUB_TYPE_MAP[match.group("region").lower()],
    normalize_basename=unquote,
)
