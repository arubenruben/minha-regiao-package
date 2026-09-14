import asyncio
import json
from pathlib import Path

from scrapling.fetchers import AsyncStealthySession
from tqdm import tqdm

SNIT_PORTAL_URL = "https://snit-mais.dgterritorio.gov.pt/portalsnit/"
SNIT_SEARCH_PATH = "/portalsnit/AdvancedMetadataSearch.WebClient.ashx"
SNIT_CATALOGUE_ID = "c50892d1-6bcc-467c-a01b-6a17ceb51c2d"
SNIT_REGULAMENTO_PATH = "/portalsnit/ConfigureCSWHandler.WebClient.ashx"

PDM_TITLE_PREFIX = "Plano Diretor Municipal"

MUNICIPALITIES_FILE = (
    Path(__file__).with_name("data") / "GetRegionsAndMunicipalitiesAsync.json"
)


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


async def _run_in_page(session: AsyncStealthySession, script: str, arg: dict) -> dict:
    captured: dict = {}

    async def page_action(page):
        captured["result"] = await page.evaluate(script, arg)
        return page

    await session.fetch(SNIT_PORTAL_URL, page_action=page_action, network_idle=True)

    return json.loads(captured["result"]["text"])


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


async def fetch_pdm_pdf_urls(
    session: AsyncStealthySession, identifier: str
) -> list[str]:
    payload = await _run_in_page(
        session,
        REGULAMENTO_SCRIPT,
        {"path": SNIT_REGULAMENTO_PATH, "idMetadata": identifier},
    )
    return [entry["PDF"] for entry in payload["lDinRegValues"]]


async def main() -> None:
    municipalities = load_municipalities()

    async with AsyncStealthySession(headless=True, network_idle=True) as session:
        with tqdm(municipalities, desc="Searching SNIT", unit="city") as progress:
            for municipio in progress:
                progress.set_postfix(municipio=municipio)
                try:
                    payload = await search_municipio(session, municipio)
                except Exception:
                    tqdm.write(f"{municipio}: search failed")
                    continue

                pdm = next(
                    (
                        record
                        for record in payload.get("results", [])
                        if record["Type"] == "series"
                        and record["Title"].startswith(PDM_TITLE_PREFIX)
                    ),
                    None,
                )
                if pdm is None:
                    tqdm.write(f"{municipio}: no PDM record")
                    continue

                try:
                    pdf_urls = await fetch_pdm_pdf_urls(session, pdm["Identifier"])
                except Exception:
                    tqdm.write(f"{municipio}: regulamento lookup failed")
                    continue

                tqdm.write(f"\n{municipio}: {len(pdf_urls)} PDF(s)")

                for url in pdf_urls:
                    tqdm.write(f"  - {url}")

                break


if __name__ == "__main__":
    asyncio.run(main())
