import re

# Document names follow "<Tipo> n.º <número>/<ano>, de <data>" (e.g. "Aviso
# n.º 12231/2017, de 12 de outubro"); the year is the last 4-digit group.
_YEAR_PATTERN = re.compile(r"/(\d{4})")


def extract_year(name: str) -> int | None:
    match = _YEAR_PATTERN.search(name)
    return int(match.group(1)) if match else None


def is_complete(name: str) -> bool:
    """A document is only "complete" (i.e. an actual regulation, as opposed
    to a notice/edict announcing one) when its name starts with "Regulamento".
    """
    return name.strip().startswith("Regulamento")


# Same "<Tipo> n.º <número>/<ano>[/<suffix>]" shape as _YEAR_PATTERN, but
# captures the heading phrase and number too, since minha_regiao.gazette.
# GazetteSegmenter.find_notice_text_by_heading needs all three to locate
# this document's own notice inside its (multi-notice) gazette PDF page.
_NOTICE_METADATA_PATTERN = re.compile(
    r"^(?P<heading>.+?)\s+n\.?\s*[ºo]\s*(?P<number>\d+)\s*/\s*(?P<year>\d{4})(?:\s*/\s*(?P<suffix>\d+))?",
    re.IGNORECASE,
)

# Strips a parenthetical qualifier dre.pt sometimes prints between the
# heading and "n.º" (e.g. "Aviso (extrato)"), so the returned heading
# matches the canonical phrase used inside the notice's own PDF header.
_QUALIFIER_SUFFIX_RE = re.compile(r"\s*\([^)]*\)\s*$")


def parse_notice_metadata(name: str) -> tuple[str, str, int, int | None] | None:
    """Parses `name` (a document name as printed on the RMUE listing page,
    e.g. "Aviso n.º 12231/2017, de 12 de outubro") into
    (heading_phrase, number, year, suffix). Returns None when `name` doesn't
    match the expected shape at all -- the caller must not swallow that.
    """
    match = _NOTICE_METADATA_PATTERN.match(name.strip())
    if not match:
        return None

    heading = _QUALIFIER_SUFFIX_RE.sub("", match.group("heading")).strip()
    suffix = int(match.group("suffix")) if match.group("suffix") else None
    return heading, match.group("number"), int(match.group("year")), suffix
