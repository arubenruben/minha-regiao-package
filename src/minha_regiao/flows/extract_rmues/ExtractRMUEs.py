import asyncio

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from scrapling.fetchers import AsyncStealthySession, StealthyFetcher
from scrapling.parser import Selector

from minha_regiao.flows.extract_rmues.Settings import settings
from minha_regiao.flows.extract_rmues.schema.PendingDocument import PendingDocument
from minha_regiao.flows.extract_rmues.schema.RMUERegulation import RMUERegulation
from minha_regiao.flows.extract_rmues.services.PDFResolver import resolve_pdf_url
from minha_regiao.flows.extract_rmues.services.RMUEPageParser import parse_rmue_regulations
from minha_regiao.flows.extract_rmues.services.RMURepository import (
    find_documents_missing_pdf_url,
    persist_rmue_regulations,
    update_pdf_urls,
)

# How many DR detail pages to resolve per batch, and concurrently within
# that batch, through a single shared browser (as tabs in its page pool).
# Each batch is persisted before moving on to the next one, so a failure
# partway through only loses the in-flight batch, not everything resolved
# so far.
PDF_BATCH_SIZE = 8


@task(name="fetch_rmue_page")
async def fetch_rmue_page(url: str) -> Selector:
    logger = get_run_logger()
    logger.info(f"Fetching RMUE page from {url}")

    page = await StealthyFetcher.async_fetch(url, headless=True, network_idle=True)

    logger.info("Fetched RMUE page")
    return page


@task(name="parse_rmue_page", cache_policy=NO_CACHE)
async def parse_rmue_page(page: Selector) -> list[RMUERegulation]:
    logger = get_run_logger()

    entries = parse_rmue_regulations(page)

    logger.info(f"Parsed {len(entries)} municipalities from the RMUE page")
    return entries


@task(name="persist_rmue_regulations")
async def persist_rmue_page(entries: list[RMUERegulation]) -> int:
    logger = get_run_logger()

    persisted, unmatched_cities, skipped_documents = await persist_rmue_regulations(settings.database_url, entries)

    logger.info(f"Persisted {persisted} regulation documents")
    if unmatched_cities:
        logger.warning(f"Unmatched municipalities: {sorted(unmatched_cities)}")
    if skipped_documents:
        logger.warning(f"Skipped documents (no parseable year): {sorted(skipped_documents)}")

    return persisted


@task(name="find_documents_missing_pdf_url")
async def find_pending_documents() -> list[PendingDocument]:
    logger = get_run_logger()

    documents = await find_documents_missing_pdf_url(settings.database_url)

    logger.info(f"Found {len(documents)} documents missing a PDF url")
    return documents


async def _resolve_document(session: AsyncStealthySession, document: PendingDocument, logger) -> PendingDocument:
    try:
        pdf_url = await resolve_pdf_url(session, document.dre_url)
    except Exception:
        logger.warning(f"Failed to fetch {document.dre_url}", exc_info=True)
        pdf_url = None

    if pdf_url is None:
        logger.warning(f"No PDF link found on {document.dre_url}")

    return document.model_copy(update={"pdf_url": pdf_url})


@task(name="resolve_pdf_urls")
async def resolve_pdf_urls(documents: list[PendingDocument]) -> int:
    logger = get_run_logger()

    persisted = 0
    async with AsyncStealthySession(headless=True, network_idle=True, max_pages=PDF_BATCH_SIZE) as session:
        for start in range(0, len(documents), PDF_BATCH_SIZE):
            batch = documents[start : start + PDF_BATCH_SIZE]

            resolved = await asyncio.gather(*(_resolve_document(session, document, logger) for document in batch))
            updated = await update_pdf_urls(settings.database_url, resolved)
            persisted += updated

            batch_number = start // PDF_BATCH_SIZE + 1
            logger.info(f"Batch {batch_number}: persisted {updated}/{len(batch)} PDF urls")

    logger.info(f"Resolved and persisted {persisted}/{len(documents)} PDF urls in total")
    return persisted


@flow(
    name="extract_rmues",
    description="Extract and index RMUE and municipal fee regulations from the Diário da República website by city.",
)
async def extract_rmues(base_url: str) -> int:
    page = await fetch_rmue_page(base_url)
    entries = await parse_rmue_page(page)
    await persist_rmue_page(entries)

    pending_documents = await find_pending_documents()
    return await resolve_pdf_urls(pending_documents)


if __name__ == "__main__":
    asyncio.run(extract_rmues(base_url=settings.rmue_url))
