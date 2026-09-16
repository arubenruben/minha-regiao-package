import asyncio
from typing import cast

from prefect import flow, get_run_logger
from tqdm import tqdm

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_rmues.services.ResolutionStore import ResolutionStore
from extract_rmues.Settings import settings
from extract_rmues.tasks.ExtractNoticeText import (
    EXTRACT_NOTICE_TEXT_TAG,
    extract_notice_text_task,
)
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
    documents, then resolves each document's PDF url. The resolve step fans
    out one Prefect task run per document via `.map()`, capped at
    `settings.pdf_resolve_concurrency` concurrently running task runs (see
    the tag-based concurrency limit registered below) -- this itself isn't
    gated by `load_targets`, since it's a required enrichment rather than a
    load step. Its document source is gated, though: with `"database"` in
    `load_targets` it re-queries every document (from this run and any
    prior run) still missing a `pdf_url` in Postgres and persists resolved
    urls back there; otherwise it operates purely on this run's
    freshly-parsed `entries` in memory, so the flow needs no database at
    all -- in that case, each document already resolved by a *previous* run
    is loaded from `settings.resolution_state_file` (see
    extract_rmues.services.ResolutionStore) and skipped rather than
    reopening a browser for it again.
    """
    logger = get_run_logger()

    page = await fetch_rmue_page_task(base_url)
    entries = await parse_rmue_page_task(page)

    if "database" in settings.load_targets:
        await persist_rmue_page_task(entries)
    if "json" in settings.load_targets:
        await write_rmue_page_json_task(entries)

    pending_documents = await find_pending_documents_task(entries)

    # Only meaningful (and only touched) when "database" isn't in
    # load_targets -- Postgres is already the source of truth for
    # already-resolved documents otherwise.
    resolution_store = (
        ResolutionStore(settings.resolution_state_file)
        if "database" not in settings.load_targets
        else None
    )

    if resolution_store is not None:
        already_resolved = {
            document.dre_url: cached
            for document in pending_documents
            if (cached := resolution_store.get(document.dre_url)) is not None
        }
        to_resolve = [
            document
            for document in pending_documents
            if document.dre_url not in already_resolved
        ]
    else:
        already_resolved = {}
        to_resolve = pending_documents

    await ensure_concurrency_limit(
        RESOLVE_PDF_URL_TAG, settings.pdf_resolve_concurrency
    )

    futures = resolve_pdf_url_task.map(to_resolve)

    newly_resolved: list[PendingDocument] = []

    with tqdm(
        total=len(to_resolve), desc="Resolving PDF urls", unit="doc"
    ) as progress:
        for future in futures:
            newly_resolved.append(cast("PendingDocument", future.result()))
            progress.update(1)

    if resolution_store is not None:
        resolution_store.record(newly_resolved)
        if already_resolved:
            logger.info(
                f"Reused {len(already_resolved)} previously resolved PDF url(s)"
            )

    resolved_by_url = {
        **already_resolved,
        **{document.dre_url: document for document in newly_resolved},
    }
    resolved = [resolved_by_url[document.dre_url] for document in pending_documents]

    updated = await update_pdf_urls_task(resolved)
    logger.info(f"Resolved {updated}/{len(pending_documents)} PDF urls in total")

    # Opens each resolved PDF and narrows it down to its own notice text via
    # minha_regiao.gazette.GazetteSegmenter -- same shared logic extract_pdms
    # uses, since these PDFs are raw gazette page ranges too (see
    # extract_rmues.tasks.ExtractNoticeText). Not yet persisted anywhere:
    # RMUE/FeeRegulation have no column for it yet, so this only proves the
    # extraction works end to end -- storage is a separate follow-up.
    resolvable = [document for document in resolved if document.pdf_url is not None]

    await ensure_concurrency_limit(
        EXTRACT_NOTICE_TEXT_TAG, settings.pdf_extract_concurrency
    )

    text_futures = extract_notice_text_task.map(resolvable)

    extracted = 0

    with tqdm(
        total=len(resolvable), desc="Extracting notice text", unit="doc"
    ) as progress:
        for future in text_futures:
            if cast("str | None", future.result()) is not None:
                extracted += 1
            progress.update(1)

    logger.info(
        f"Extracted notice text for {extracted}/{len(resolvable)} resolved documents"
    )

    return updated


if __name__ == "__main__":
    asyncio.run(extract_rmues(base_url=settings.rmue_url))
