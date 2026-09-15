import json
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

from prefect import get_run_logger
from scrapling.fetchers import AsyncStealthySession

import extract_pdms

SNIT_PORTAL_URL = "https://snit-mais.dgterritorio.gov.pt/portalsnit/"
SNIT_SEARCH_PATH = "/portalsnit/AdvancedMetadataSearch.WebClient.ashx"
SNIT_CATALOGUE_ID = "c50892d1-6bcc-467c-a01b-6a17ceb51c2d"
SNIT_REGULAMENTO_PATH = "/portalsnit/ConfigureCSWHandler.WebClient.ashx"

PDM_TITLE_PREFIX = "Plano Diretor Municipal"

# Resolved against the extract_pdms package root (not this file's own
# location, which is one level down under services/) rather than
# Path(__file__).with_name("data").
MUNICIPALITIES_FILE = (
    Path(extract_pdms.__file__).with_name("data") / "GetRegionsAndMunicipalitiesAsync.json"
)

# SNIT mirrors dre.pt's own filename convention for regulation PDFs, e.g.
# "AVISO 3341_2012.pdf", "DECL RET 1190_2014.pdf" or "AVISO 16460_2024_2.pdf"
# (the trailing "_2" marks a re-published/corrected copy of the same diploma).
PDF_FILENAME_PATTERN = re.compile(
    r"^(?P<doc_type>[A-Z]+(?: [A-Z]+)*) (?P<number>\d+)_(?P<year>\d{4})(?:_(?P<suffix>\d+))?$"
)


class PdfUrlParseError(ValueError):
    """Raised when a SNIT regulation PDF URL doesn't match the expected dre.pt-style filename convention."""


class SnitPageActionError(RuntimeError):
    """Raised when the portal page doesn't produce a result for our
    page_action -- e.g. scrapling itself logs a page_action failure (seen in
    practice as the portal serving an error page with no __wc_csrfToken
    element, most likely anti-bot/rate-limiting) and swallows it, leaving
    the page in an unusable state instead of propagating the failure.
    """


def load_municipalities() -> list[str]:
    regions = json.loads(MUNICIPALITIES_FILE.read_text(encoding="utf-8"))
    return [
        city["designation"]
        for region in regions
        for city in region["listMunicipalities"]
    ]


# The search endpoint checks a double-submit CSRF token/cookie pair minted
# when the portal page loads, plus server-side session state set up by the
# portal's own JS boot sequence -- replaying the token with a plain HTTP
# request gets "Sessão expirada" back. So the search is issued as an in-page
# fetch() against an actually-loaded portal page (via page_action) instead.
SEARCH_SCRIPT = """
async ({ catalogue, path, municipio }) => {
    const token = document.getElementById('__wc_csrfToken').value;
    const params = new URLSearchParams({
        catalogue,
        profile: 'CSW',
        identifier: '_false',
        searchmode: 'simple',
        info: 'IGT',
        planos: '',
        municipio,
        type: 'series;service',
        start: '0',
        limit: '300',
        __wc_csrfToken: token,
    });
    const res = await fetch(path, {
        method: 'POST',
        headers: {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'x-requested-with': 'XMLHttpRequest',
        },
        body: params.toString(),
        credentials: 'same-origin',
    });
    return { status: res.status, text: await res.text() };
}
"""

# Second call, fired once a "Plano Diretor Municipal" record has been found
# by SEARCH_SCRIPT: given that record's Identifier as idMetadata, this lists
# every regulation PDF published against it (revisions, corrections,
# amendments, ...) as lDinRegValues[].PDF.
REGULAMENTO_SCRIPT = """
async ({ path, idMetadata }) => {
    const token = document.getElementById('__wc_csrfToken').value;
    const params = new URLSearchParams({
        action: 'getDinamicaAndRegulamento',
        sType: 'Reg',
        idMetadata,
        __wc_csrfToken: token,
    });
    const res = await fetch(path, {
        method: 'POST',
        headers: {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'x-requested-with': 'XMLHttpRequest',
        },
        body: params.toString(),
        credentials: 'same-origin',
    });
    return { status: res.status, text: await res.text() };
}
"""


class _PageEvaluator:
    """Callable page_action for AsyncStealthySession.fetch(): the library
    calls it with a single `page` argument, so `script`/`arg` and the
    captured result are held as attributes instead of a closure.
    """

    def __init__(self, script: str, arg: dict) -> None:
        self.script = script
        self.arg = arg
        self.result: dict | None = None

    async def __call__(self, page):
        self.result = await page.evaluate(self.script, self.arg)
        return page


async def _run_in_page(session: AsyncStealthySession, script: str, arg: dict) -> dict:
    evaluator = _PageEvaluator(script, arg)
    await session.fetch(SNIT_PORTAL_URL, page_action=evaluator, network_idle=True)

    if evaluator.result is None:
        raise SnitPageActionError(f"No result from page_action against {SNIT_PORTAL_URL} -- portal page likely failed to load")

    return json.loads(evaluator.result["text"])


async def search_municipio(session: AsyncStealthySession, municipio: str) -> dict:
    return await _run_in_page(
        session,
        SEARCH_SCRIPT,
        {
            "catalogue": SNIT_CATALOGUE_ID,
            "path": SNIT_SEARCH_PATH,
            "municipio": municipio,
        },
    )


def parse_pdf_metadata(url: str) -> dict:
    """Parses the dre.pt-style filename SNIT mirrors for regulation PDFs
    (e.g. ".../AVISO 3341_2012.pdf", ".../DECL RET 1190_2014.pdf",
    ".../AVISO 16460_2024_2.pdf") into its document type, number, year and
    optional revision suffix. Raises PdfUrlParseError on any URL that doesn't
    match the convention -- the caller must not swallow it.
    """
    stem = Path(unquote(urlparse(url).path)).stem
    match = PDF_FILENAME_PATTERN.match(stem)

    if not match:
        raise PdfUrlParseError(f"Could not parse PDF filename metadata from URL: {url}")

    return {
        "url": url,
        "doc_type": match.group("doc_type"),
        "number": match.group("number"),
        "year": int(match.group("year")),
        "suffix": int(match.group("suffix")) if match.group("suffix") else None,
    }


def build_regulamento_entry(entry: dict) -> dict:
    """Merges the parsed PDF-filename metadata with the raw lDinRegValues
    fields SNIT returns for that regulation (publication date, dynamics,
    publication reference).
    """
    return {
        **parse_pdf_metadata(entry["PDF"]),
        "data_publicacao": entry.get("DataPublicacao"),
        "dinamica": entry.get("Dinamica"),
        "publicacao": entry.get("Publicacao"),
    }


async def fetch_pdm_pdf_urls(
    session: AsyncStealthySession, identifier: str
) -> list[dict]:
    """Fetches every regulation entry for `identifier`, skipping (and
    logging) any single entry whose PDF URL doesn't match the expected
    filename convention -- one malformed entry must not discard the rest of
    an otherwise valid regulation history.
    """
    logger = get_run_logger()

    payload = await _run_in_page(
        session,
        REGULAMENTO_SCRIPT,
        {"path": SNIT_REGULAMENTO_PATH, "idMetadata": identifier},
    )

    documents: list[dict] = []
    for entry in payload["lDinRegValues"]:
        try:
            documents.append(build_regulamento_entry(entry))
        except PdfUrlParseError as error:
            logger.error(f"Skipping regulation document for {identifier}: {error}")

    return documents
