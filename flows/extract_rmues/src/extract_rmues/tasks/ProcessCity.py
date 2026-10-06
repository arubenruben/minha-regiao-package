from typing import cast

from prefect import get_run_logger, task

from extract_rmues.schema.RMUERegulation import (
    DocumentStatus,
    RegulationDocument,
    RMUERegulation,
)
from extract_rmues.services.OutputStore import OutputStore
from extract_rmues.tasks.ExtractNoticeText import extract_notice_text_task
from extract_rmues.tasks.ResolvePdfUrl import resolve_pdf_url_task

# Paired with a Prefect tag-based concurrency limit registered by the caller
# (see extract_rmues.ExtractRMUEs), sized directly from
# settings.city_concurrency -- that one setting is the number of
# municipalities processed in parallel.
PROCESS_CITY_TAG = "rmue-city-pipeline"


def _find_processed(
    dre_url: str, output_store: OutputStore, processed_by_url: dict[str, RegulationDocument]
) -> RegulationDocument | None:
    """The `OutputStore` is looked up first, then `processed_by_url`."""
    cached = output_store.get_document(dre_url)
    return cached if cached is not None else processed_by_url.get(dre_url)


async def _resolve_and_extract(
    documents: list[RegulationDocument],
    output_store: OutputStore,
    processed_by_url: dict[str, RegulationDocument],
) -> list[RegulationDocument]:
    """Resolves each document's PDF url and extracts its own notice
    text/structure, skipping any document already recorded in
    `output_store` or, failing that, in `processed_by_url` (the documents
    already persisted in the database, see
    extract_rmues.services.RMURepository.find_processed_documents) --
    idempotence here is per document (by dre_url), mirroring
    extract_pdms.services.MunicipioPipeline._extract_documents: a
    document's PDF resolution + text extraction is the expensive,
    failure-prone step worth not repeating on a re-run, regardless of
    whether it previously succeeded or failed.

    Returns one RegulationDocument per entry in `documents`, in the same
    order: the one recorded in `output_store` or `processed_by_url` when
    there is one, otherwise the freshly resolved/extracted result. A
    document whose PDF url couldn't be resolved comes back with
    `status=PDF_URL_NOT_FOUND`.
    """
    already_processed = {
        document.dre_url: cached
        for document in documents
        if (cached := _find_processed(document.dre_url, output_store, processed_by_url)) is not None
    }
    to_resolve = [document for document in documents if document.dre_url not in already_processed]

    resolved = (
        cast("list[RegulationDocument]", resolve_pdf_url_task.map(to_resolve).result()) if to_resolve else []
    )
    resolved_by_url = {document.dre_url: document for document in resolved}

    to_extract = [document for document in resolved if document.pdf_url is not None]
    extracted = (
        cast("list[RegulationDocument]", extract_notice_text_task.map(to_extract).result()) if to_extract else []
    )
    extracted_by_url = {document.dre_url: document for document in extracted}

    results: list[RegulationDocument] = []

    for document in documents:
        cached = already_processed.get(document.dre_url)
        if cached is not None:
            results.append(cached)
            continue

        resolved_document = resolved_by_url[document.dre_url]
        if resolved_document.pdf_url is None:
            results.append(resolved_document.model_copy(update={"status": DocumentStatus.PDF_URL_NOT_FOUND}))
        else:
            results.append(extracted_by_url[document.dre_url])

    return results


@task(name="process_city", tags=[PROCESS_CITY_TAG], persist_result=False)
async def process_city_task(
    entry: RMUERegulation,
    output_store: OutputStore,
    processed_by_url: dict[str, RegulationDocument],
) -> RMUERegulation:
    """One municipality's full pipeline: resolves each of its documents'
    PDF urls, then extracts their own notice text and legal structure --
    mapped once per municipality (see extract_rmues.ExtractRMUEs), so this
    municipality's resolution/extraction runs concurrently with others in
    flight, each still capped by its own tag-based concurrency limit (see
    extract_rmues.tasks.ResolvePdfUrl.RESOLVE_PDF_URL_TAG and
    extract_rmues.tasks.ExtractNoticeText.EXTRACT_NOTICE_TEXT_TAG)
    independently of how many municipalities are processed at once.

    `entry` is this municipality's full document set as parsed off the
    listing page in the current run (see
    extract_rmues.tasks.ParseRmuePage), so the returned entry is already
    this city's complete set of documents, each with its final
    pdf_url/status/raw_text/structure -- there's nothing to merge with
    what `output_store` already has for it. It's recorded there (persisting
    it to disk immediately -- see OutputStore.record) before returning.

    `processed_by_url` is the documents already persisted in the database
    (see extract_rmues.tasks.FindProcessedDocuments), the same read-only dict
    for every mapped call; the caller wraps it in `quote` so Prefect doesn't
    walk its whole contents again for each task run.
    """
    logger = get_run_logger()

    resolved_entry = RMUERegulation(
        municipality=entry.municipality,
        urbanization_documents=await _resolve_and_extract(
            entry.urbanization_documents, output_store, processed_by_url
        ),
        fee_documents=await _resolve_and_extract(entry.fee_documents, output_store, processed_by_url),
    )
    output_store.record(resolved_entry)

    document_count = len(resolved_entry.urbanization_documents) + len(resolved_entry.fee_documents)
    logger.info(f"{entry.municipality}: processed {document_count} document(s)")
    return resolved_entry
