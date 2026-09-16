import asyncio
from typing import cast

from prefect import flow, get_run_logger
from tqdm import tqdm

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_rmues.Settings import settings
from extract_rmues.tasks.FetchRmuePage import fetch_rmue_page_task
from extract_rmues.tasks.FindPendingDocuments import find_pending_documents_task
from extract_rmues.tasks.ParseRmuePage import parse_rmue_page_task
from extract_rmues.tasks.PersistRmueRegulations import persist_rmue_page_task
from extract_rmues.tasks.ResolvePdfUrl import RESOLVE_PDF_URL_TAG, resolve_pdf_url_task
from extract_rmues.tasks.UpdatePdfUrls import update_pdf_urls_task
from extract_rmues.tasks.WriteRmueJson import write_rmue_page_json_task


@flow(
    name="extract_rmues",
    description="Extract and index RMUE and municipal fee regulations from the Diário da República website by city.",
)
async def extract_rmues(base_url: str) -> int:
    """Parses the DR listing page, persists the resulting RMUE/fee-regulation
    documents, then resolves each persisted document's PDF url. The resolve
    step fans out one Prefect task run per document via `.map()`, capped at
    `settings.pdf_resolve_concurrency` concurrently running task runs (see
    the tag-based concurrency limit registered below) -- this isn't gated by
    `load_targets` since it's a required enrichment of already-persisted
    rows rather than a load step.
    """
    logger = get_run_logger()

    page = await fetch_rmue_page_task(base_url)
    entries = await parse_rmue_page_task(page)

    if "database" in settings.load_targets:
        await persist_rmue_page_task(entries)
    if "json" in settings.load_targets:
        await write_rmue_page_json_task(entries)

    pending_documents = await find_pending_documents_task()

    await ensure_concurrency_limit(RESOLVE_PDF_URL_TAG, settings.pdf_resolve_concurrency)

    futures = resolve_pdf_url_task.map(pending_documents)

    resolved: list[PendingDocument] = []
    with tqdm(total=len(pending_documents), desc="Resolving PDF urls", unit="doc") as progress:
        for future in futures:
            resolved.append(cast("PendingDocument", future.result()))
            progress.update(1)

    updated = await update_pdf_urls_task(resolved)
    logger.info(f"Resolved and persisted {updated}/{len(pending_documents)} PDF urls in total")
    return updated


if __name__ == "__main__":
    asyncio.run(extract_rmues(base_url=settings.rmue_url))
