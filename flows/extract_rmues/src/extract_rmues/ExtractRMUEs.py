import asyncio
from collections import defaultdict
from typing import cast

from prefect import flow, get_run_logger, unmapped
from tqdm import tqdm

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.services.ConcurrencyLimiter import ensure_concurrency_limit
from extract_rmues.services.OutputStore import OutputStore
from extract_rmues.Settings import settings
from extract_rmues.tasks.ExtractNoticeText import EXTRACT_NOTICE_TEXT_TAG
from extract_rmues.tasks.FetchRmuePage import fetch_rmue_page_task
from extract_rmues.tasks.FindPendingDocuments import find_pending_documents_task
from extract_rmues.tasks.ParseRmuePage import parse_rmue_page_task
from extract_rmues.tasks.PersistRmueRegulations import persist_rmue_page_task
from extract_rmues.tasks.ProcessCity import PROCESS_CITY_TAG, process_city_task
from extract_rmues.tasks.ResolvePdfUrl import RESOLVE_PDF_URL_TAG
from extract_rmues.tasks.UpdatePdfUrls import update_pdf_urls_task
from extract_rmues.tasks.WriteRmueJson import write_rmue_page_json_task


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

    Idempotent and resumable, at the document level: `output_store`
    persists each document's PDF-resolution/text-extraction result --
    keyed by dre_url, via its own locked critical section (see
    OutputStore.record) -- to `settings.state_file` as soon as each city
    finishes, and a document already recorded there (regardless of whether
    it succeeded) is reused on the next run instead of being re-resolved
    and re-extracted. See extract_rmues.tasks.ProcessCity.
    """
    logger = get_run_logger()

    page = await fetch_rmue_page_task(base_url)
    entries = await parse_rmue_page_task(page)

    if "database" in settings.load_targets:
        await persist_rmue_page_task(entries)

    # Only meaningful (and only touched) when "database" is in
    # load_targets: this returns every document still missing a PDF url in
    # Postgres, including backlog from prior runs. Otherwise falls back to
    # this run's freshly-parsed entries in-memory, so the flow needs no
    # database at all.
    pending_documents = await find_pending_documents_task(entries)

    documents_by_municipality: dict[str, list[PendingDocument]] = defaultdict(list)
    for document in pending_documents:
        documents_by_municipality[document.municipality].append(document)
    municipalities = list(documents_by_municipality.items())

    output_store = OutputStore(settings.state_file)

    await ensure_concurrency_limit(PROCESS_CITY_TAG, settings.city_concurrency)
    await ensure_concurrency_limit(RESOLVE_PDF_URL_TAG, settings.pdf_resolve_concurrency)
    await ensure_concurrency_limit(EXTRACT_NOTICE_TEXT_TAG, settings.pdf_extract_concurrency)

    futures = process_city_task.map(
        [municipality for municipality, _ in municipalities],
        [documents for _, documents in municipalities],
        unmapped(output_store),
    )

    resolved: list[PendingDocument] = []

    with tqdm(total=len(municipalities), desc="Processing cities", unit="city") as progress:
        for future in futures:
            resolved.extend(cast("list[PendingDocument]", future.result()))
            progress.update(1)

    updated = await update_pdf_urls_task(resolved)
    logger.info(f"Resolved {updated}/{len(pending_documents)} PDF urls in total")

    if "json" in settings.load_targets:
        await write_rmue_page_json_task(output_store.records)

    return updated


if __name__ == "__main__":
    asyncio.run(extract_rmues(base_url=settings.rmue_url))
