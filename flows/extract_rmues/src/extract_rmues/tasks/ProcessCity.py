from typing import cast

from prefect import get_run_logger, task

from extract_rmues.schema.PendingDocument import PendingDocument
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


def _merge_documents(
    existing: list[RegulationDocument], updates: dict[str, RegulationDocument]
) -> list[RegulationDocument]:
    """Returns `existing` with any document also present in `updates`
    replaced by its updated copy, plus any brand-new document in `updates`
    appended -- so a run that only touches a subset of a city's documents
    (e.g. only those still missing a PDF url in Postgres) doesn't drop the
    rest of that city's already-recorded documents from the output.
    """
    merged = [updates.get(document.dre_url, document) for document in existing]
    seen = {document.dre_url for document in existing}
    merged.extend(document for dre_url, document in updates.items() if dre_url not in seen)
    return merged


async def _resolve_and_extract(
    documents: list[PendingDocument], output_store: OutputStore
) -> tuple[dict[str, RegulationDocument], list[PendingDocument]]:
    """Resolves each document's PDF url and extracts its own notice
    text/structure, skipping any document already recorded in
    `output_store` -- idempotence here is per document (by dre_url),
    mirroring extract_pdms.services.MunicipioPipeline._extract_documents: a
    document's PDF resolution + text extraction is the expensive,
    failure-prone step worth not repeating on a re-run, regardless of
    whether it previously succeeded or failed.

    Returns the resulting RegulationDocuments keyed by dre_url, alongside
    every document's resolved PendingDocument (cached or freshly resolved)
    so the caller can persist pdf_url back to Postgres.
    """
    already_processed = {
        document.dre_url: cached
        for document in documents
        if (cached := output_store.get_document(document.dre_url)) is not None
    }
    to_resolve = [document for document in documents if document.dre_url not in already_processed]

    newly_resolved = (
        cast("list[PendingDocument]", resolve_pdf_url_task.map(to_resolve).result()) if to_resolve else []
    )
    resolved_by_url = {document.dre_url: document for document in newly_resolved}

    to_extract = [document for document in newly_resolved if document.pdf_url is not None]
    extracted = (
        cast("list[RegulationDocument]", extract_notice_text_task.map(to_extract).result()) if to_extract else []
    )
    extracted_by_url = {regulation.dre_url: regulation for regulation in extracted}

    regulations_by_url: dict[str, RegulationDocument] = {}
    resolved_documents: list[PendingDocument] = []

    for document in documents:
        cached = already_processed.get(document.dre_url)
        if cached is not None:
            regulations_by_url[document.dre_url] = cached
            resolved_documents.append(document.model_copy(update={"pdf_url": cached.pdf_url}))
            continue

        resolved_document = resolved_by_url[document.dre_url]
        resolved_documents.append(resolved_document)

        if resolved_document.pdf_url is None:
            regulations_by_url[document.dre_url] = RegulationDocument(
                name=document.name, dre_url=document.dre_url, status=DocumentStatus.PDF_URL_NOT_FOUND
            )
        else:
            regulations_by_url[document.dre_url] = extracted_by_url[document.dre_url]

    return regulations_by_url, resolved_documents


@task(name="process_city", tags=[PROCESS_CITY_TAG], persist_result=False)
async def process_city_task(
    municipality: str,
    documents: list[PendingDocument],
    output_store: OutputStore,
) -> list[PendingDocument]:
    """One municipality's full pipeline: resolves each of its pending
    documents' PDF urls, then extracts their own notice text and legal
    structure -- mapped once per municipality (see
    extract_rmues.ExtractRMUEs), so this municipality's resolution/
    extraction runs concurrently with others in flight, each still capped
    by its own tag-based concurrency limit (see
    extract_rmues.tasks.ResolvePdfUrl.RESOLVE_PDF_URL_TAG and
    extract_rmues.tasks.ExtractNoticeText.EXTRACT_NOTICE_TEXT_TAG)
    independently of how many municipalities are processed at once.

    `documents` is only this run's pending set (e.g. just the documents
    still missing a PDF url in Postgres, not this city's full history), so
    the resulting record is merged onto whatever `output_store` already
    has for `municipality` -- see `_merge_documents` -- rather than
    replacing it outright, then recorded (persisting it to disk
    immediately -- see OutputStore.record) before returning.

    Returns the resolved PendingDocuments (pdf_url filled in, whether newly
    resolved or reused from `output_store`) so the caller can persist
    pdf_url back to Postgres when `"database"` is in load_targets.
    """
    logger = get_run_logger()

    regulations_by_url, resolved_documents = await _resolve_and_extract(documents, output_store)

    urbanization_updates = {
        document.dre_url: regulations_by_url[document.dre_url] for document in documents if document.table == "rmue"
    }
    fee_updates = {
        document.dre_url: regulations_by_url[document.dre_url]
        for document in documents
        if document.table == "fee_regulation"
    }

    existing = output_store.get_entry(municipality)
    entry = RMUERegulation(
        municipality=municipality,
        urbanization_documents=_merge_documents(
            existing.urbanization_documents if existing else [], urbanization_updates
        ),
        fee_documents=_merge_documents(existing.fee_documents if existing else [], fee_updates),
    )
    output_store.record(entry)

    logger.info(f"{municipality}: processed {len(documents)} document(s)")
    return resolved_documents
