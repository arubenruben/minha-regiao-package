import unicodedata
from urllib.parse import unquote, urlparse

# "plano director municipal" is the pre-orthographic-reform spelling still
# used verbatim on some older town hall sites, so it's matched alongside the
# current "diretor" spelling.
_PDM_KEYWORDS = ("pdm", "plano diretor municipal", "plano director municipal")
_REGULATION_KEYWORD = "regulamento"

# Broader terms under which a site's navigation typically leads to its PDM
# section, used only to decide which links are worth following, not as a
# match for the PDM itself.
_NAV_KEYWORDS = _PDM_KEYWORDS + (
    "urbanismo",
    "planeamento",
    "ordenamento do territorio",
    "instrumentos de gestao territorial",
    "regulamentos",
    _REGULATION_KEYWORD,
)

_NON_PAGE_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".css", ".js", ".zip", ".rar",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".mp3", ".mp4", ".avi", ".pdf",
)


_WORD_SEPARATORS = ("_", "-", "+", ".", "/")


def _normalize(text: str) -> str:
    """Lowercases, strips accents, and turns filename/URL word separators
    into spaces, so phrases like "plano diretor municipal" still match
    hrefs such as "Plano_Diretor_Municipal.pdf".
    """
    text = unquote(text)
    stripped = "".join(char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char))
    normalized = stripped.casefold()
    for separator in _WORD_SEPARATORS:
        normalized = normalized.replace(separator, " ")
    return " ".join(normalized.split())


def is_pdm_regulation_pdf(href: str, text: str) -> bool:
    """A link is the PDM regulation when it points at a PDF and either its
    href or its visible text names both the PDM and "regulamento" (municipal
    sites link plenty of other PDFs, so both signals are required to avoid
    picking up an unrelated regulation).
    """
    if not urlparse(href).path.lower().endswith(".pdf"):
        return False

    haystack = _normalize(f"{href} {text}")
    return any(keyword in haystack for keyword in _PDM_KEYWORDS) and _REGULATION_KEYWORD in haystack


def is_high_priority(href: str, text: str) -> bool:
    """Whether a link already names the PDM itself, as opposed to just
    plausible municipal-planning navigation."""
    haystack = _normalize(f"{href} {text}")
    return any(keyword in haystack for keyword in _PDM_KEYWORDS)


def is_worth_following(href: str, text: str) -> bool:
    path = urlparse(href).path.lower()
    if any(path.endswith(extension) for extension in _NON_PAGE_EXTENSIONS):
        return False

    haystack = _normalize(f"{href} {text}")
    return any(keyword in haystack for keyword in _NAV_KEYWORDS)
