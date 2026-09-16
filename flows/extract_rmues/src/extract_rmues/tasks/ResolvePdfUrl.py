from prefect import get_run_logger, task
from scrapling.fetchers import AsyncStealthySession

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.services.PDFResolver import resolve_pdf_url

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_rmues.ExtractRMUEs), sized from settings.pdf_resolve_concurrency.
RESOLVE_PDF_URL_TAG = "rmue-pdf-resolve"

# Each mapped call opens its own AsyncStealthySession rather than sharing
# one across calls: Prefect's task runner executes every mapped call on its
# own fresh event loop in its own thread, so a session can't be shared
# across mapped calls anyway (see extract_pdms.tasks.ProcessMunicipio for
# the same reasoning). One document is resolved at a time per call, so the
# session only ever needs a single tab.
_PAGES_PER_RESOLVE_SESSION = 1


@task(name="resolve_pdf_url", tags=[RESOLVE_PDF_URL_TAG], persist_result=False)
async def resolve_pdf_url_task(document: PendingDocument) -> PendingDocument:
    """Resolves `document`'s PDF url from its Diário da República detail
    page. A failure to resolve (page load error, button not found, no PDF
    ever fires) is recorded as `pdf_url=None` rather than raised, so one
    document failing doesn't abort the whole batch -- mirrors the "informação
    não disponibilizada" case the DR listing itself already represents as no
    document at all.
    """
    logger = get_run_logger()

    try:
        async with AsyncStealthySession(
            headless=True, network_idle=True, max_pages=_PAGES_PER_RESOLVE_SESSION
        ) as session:
            pdf_url = await resolve_pdf_url(session, document.dre_url)
    except Exception:
        logger.warning(f"Failed to resolve {document.dre_url}", exc_info=True)
        pdf_url = None

    if pdf_url is None:
        logger.warning(f"No PDF link found on {document.dre_url}")

    return document.model_copy(update={"pdf_url": pdf_url})
