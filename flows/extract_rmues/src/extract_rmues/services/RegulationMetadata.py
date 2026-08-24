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
