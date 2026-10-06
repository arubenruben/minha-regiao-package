import asyncio
from typing import cast

from prefect import flow, get_run_logger, unmapped
from prefect.utilities.annotations import quote
from tqdm import tqdm

from extract_rmues.schema.RMUERegulation import RMUERegulation
from extract_rmues.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_rmues.services.OutputStore import OutputStore
from extract_rmues.Settings import settings
from extract_rmues.tasks.ExtractNoticeText import EXTRACT_NOTICE_TEXT_TAG
from extract_rmues.tasks.FetchRmuePage import fetch_rmue_page_task
from extract_rmues.tasks.FindProcessedDocuments import find_processed_documents_task
from extract_rmues.tasks.ParseRmuePage import parse_rmue_page_task
from extract_rmues.tasks.ProcessCity import PROCESS_CITY_TAG, process_city_task
from extract_rmues.tasks.ResolvePdfUrl import RESOLVE_PDF_URL_TAG


@flow(
    name="extract_rmues",
    description="Extract and index RMUE and municipal fee regulations from the Diário da República website by city.",
)
async def extract_rmues(base_url: str) -> int:
    """Fans out one Prefect task run per municipality via `.map()`, capped
    at `settings.city_concurrency` concurrently running task runs (see the
    PROCESS_CITY_TAG concurrency limit registered below) -- mirrors
    extract_pdms.ExtractPDM.extract_pdms' per-municipality fan-out. Each
    municipality's own PDF resolution and notice-text extraction are
    further capped by their own tag-based concurrency limits
    (RESOLVE_PDF_URL_TAG / EXTRACT_NOTICE_TEXT_TAG), so the total number of
    browsers/downloads in flight stays bounded regardless of how many
    municipalities are processed at once (see
    extract_rmues.tasks.ProcessCity).

    The documents to process always come from the listing page parsed in
    this very run, regardless of `settings.load_targets`: each municipality
    is mapped as a whole, and comes back with its complete set of
    documents, each already carrying its final pdf_url/status/raw_text/
    structure. Both sinks are written incrementally, by municipality: each
    `process_city_task` persists its own city to the database and records it
    in `output_store` (the JSON output file) as its last steps, once all of
    its documents are final (see extract_rmues.tasks.ProcessCity) -- so a
    document reaches the database exactly once, already complete, never as a
    half-filled placeholder row, and an interrupted run keeps everything the
    finished cities already produced. This flow only waits for the futures
    and logs the summary.

    Idempotent and resumable, at the document level: each document's
    PDF-resolution/text-extraction result is kept, and a document already
    kept (regardless of whether it succeeded) is reused on the next run
    instead of being re-resolved and re-extracted. When `"json"` is in
    `load_targets`, `output_store` is that JSON sink and a source of
    idempotence at once: it reads `settings.output_file` when built -- keyed
    by dre_url -- and each `process_city_task` rewrites the file with its
    city as its last step, via its own locked critical section (see
    OutputStore.record), so the output file is always up to date and a
    resumed run picks up from it. When `"database"` is in `load_targets` the
    database is a second source of idempotence: `processed_by_url` -- every
    `RMUE`/`FeeRegulation` row already in Postgres, read once at the start of
    the run -- is consulted after `output_store`, and a document found in
    either is reused. A document in `processed_by_url` is also not written
    back to the database (only the others are passed to the persist task), so
    each document reaches Postgres exactly once. Without `"json"` in
    `load_targets` there's no `output_store` (it's None) and the database is
    the only source.

    Returns the number of documents whose PDF url was resolved.
    """
    logger = get_run_logger()

    processed_by_url = await find_processed_documents_task()

    page = await fetch_rmue_page_task(base_url)
    entries = await parse_rmue_page_task(page)

    output_store = OutputStore(settings.output_file) if "json" in settings.load_targets else None

    await ensure_concurrency_limit(PROCESS_CITY_TAG, settings.city_concurrency)
    await ensure_concurrency_limit(RESOLVE_PDF_URL_TAG, settings.pdf_resolve_concurrency)
    await ensure_concurrency_limit(EXTRACT_NOTICE_TEXT_TAG, settings.pdf_extract_concurrency)

    # `quote` keeps Prefect from re-walking every document in this
    # (potentially huge) dict for each mapped task run -- see
    # extract_rmues.tasks.ProcessCity.
    futures = process_city_task.map(entries, unmapped(output_store), unmapped(quote(processed_by_url)))

    resolved: list[RMUERegulation] = []

    with tqdm(total=len(entries), desc="Processing cities", unit="city") as progress:
        for future in futures:
            resolved.append(cast("RMUERegulation", future.result()))
            progress.update(1)

    documents = [document for entry in resolved for document in (*entry.urbanization_documents, *entry.fee_documents)]
    updated = sum(1 for document in documents if document.pdf_url is not None)
    logger.info(f"Resolved {updated}/{len(documents)} PDF urls in total")

    return updated


if __name__ == "__main__":
    asyncio.run(extract_rmues(base_url=settings.rmue_url))
