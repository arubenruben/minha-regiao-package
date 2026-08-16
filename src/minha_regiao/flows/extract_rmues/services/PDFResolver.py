from typing import Callable

from scrapling.fetchers import AsyncStealthySession

from minha_regiao.flows.extract_rmues.services.RMUEPageParser import parse_pdf_url

# On current DR detail pages the PDF isn't a plain link: the visible button
# has href="#" and only fires an XHR to the real file (under
# files.diariodarepublica.pt) once clicked, so it has to be intercepted.
PDF_BUTTON_SELECTOR = 'a[title*="Descarregar PDF"]'
PDF_FILE_HOST = "files.diariodarepublica.pt"


def _make_pdf_capture_action(captured: dict[str, str]) -> Callable:
    async def click_and_capture(page):
        def on_response(response):
            if PDF_FILE_HOST in response.url and response.url.lower().endswith(".pdf"):
                captured["url"] = response.url

        page.on("response", on_response)
        try:
            await page.click(PDF_BUTTON_SELECTOR, timeout=5000)
            await page.wait_for_timeout(3000)
        except Exception:
            pass

        return page

    return click_and_capture


async def resolve_pdf_url(session: AsyncStealthySession, dre_url: str) -> str | None:
    """Resolves the actual PDF file url for a DR detail page. Some older
    pages still expose it as a plain anchor (`parse_pdf_url`); current pages
    only reveal it once the "Descarregar PDF" button is clicked, which we
    handle by intercepting the XHR it fires.
    """
    captured: dict[str, str] = {}
    detail_page = await session.fetch(dre_url, page_action=_make_pdf_capture_action(captured))
    return captured.get("url") or parse_pdf_url(detail_page)
