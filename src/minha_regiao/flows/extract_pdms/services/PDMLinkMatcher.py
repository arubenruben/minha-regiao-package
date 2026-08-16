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

# Some municipal CMSes serve documents through a generic action route (e.g.
# ".../regulamento-x/download") instead of a direct file URL, giving no
# extension to key off. Navigating a browser straight to either kind of link
# triggers a file download rather than a page load, so both must be treated
# the same way: as a document, never as a page to crawl further.
_DOWNLOAD_PATH_SEGMENT = "download"

# Sections that municipal sites link to constantly (news archives, event
# calendars, ...) but which never lead toward the PDM regulation. Skipped
# outright, by URL path alone, even if a page in there happens to also match
# a _NAV_KEYWORDS term (e.g. a news article about a new "regulamento"), since
# following them just burns the page budget on an ever-repeating subtree.
_EXCLUDED_PATH_KEYWORDS = (
    "noticias",
    "eventos",
    "agenda",
    "imprensa",
    "galeria",
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


def _looks_like_document(href: str) -> bool:
    """Whether `href` points at a file to download rather than a navigable
    HTML page."""
    path = urlparse(href).path.lower()
    if any(path.endswith(extension) for extension in _NON_PAGE_EXTENSIONS):
        return True
    # Some CMSes (e.g. Joomla's docman) route downloads as
    # ".../download/<doc-id>/<category-id>/<file-id>", with "download" as a
    # middle segment rather than the last one.
    segments = [segment for segment in path.split("/") if segment]
    return _DOWNLOAD_PATH_SEGMENT in segments


def is_pdm_regulation_pdf(href: str, text: str) -> bool:
    """A link is the PDM regulation when it points at a document and either
    its href or its visible text names both the PDM and "regulamento"
    (municipal sites link plenty of other documents, so both signals are
    required to avoid picking up an unrelated regulation).
    """
    if not _looks_like_document(href):
        return False

    haystack = _normalize(f"{href} {text}")
    return any(keyword in haystack for keyword in _PDM_KEYWORDS) and _REGULATION_KEYWORD in haystack


def is_high_priority(href: str, text: str) -> bool:
    """Whether a link already names the PDM itself, as opposed to just
    plausible municipal-planning navigation."""
    haystack = _normalize(f"{href} {text}")
    return any(keyword in haystack for keyword in _PDM_KEYWORDS)


def is_worth_following(href: str, text: str) -> bool:
    if _looks_like_document(href):
        return False

    path = _normalize(urlparse(href).path)
    if any(keyword in path for keyword in _EXCLUDED_PATH_KEYWORDS):
        return False

    haystack = _normalize(f"{href} {text}")
    return any(keyword in haystack for keyword in _NAV_KEYWORDS)
