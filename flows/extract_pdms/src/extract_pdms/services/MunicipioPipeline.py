from pathlib import Path
from typing import cast

from prefect import get_run_logger, unmapped
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services import SnitSearch
from extract_pdms.services.OutputStore import OutputStore
from extract_pdms.tasks.ExtractPdfText import extract_pdf_text_task
from extract_pdms.tasks.FetchRegulationDocuments import fetch_regulation_documents_task
from extract_pdms.tasks.SearchMunicipio import search_municipio_task


async def _extract_documents(
    tmp_dir: Path,
    documents: list[RegulationDocument],
    pdf_download_timeout_seconds: float,
    output_store: OutputStore,
) -> list[RegulationDocument]:
    """Resolves `documents` to their final (downloaded + text-extracted)
    state, skipping any document already recorded in `output_store` --
    idempotence here is per document (by URL), not per municipality or per
    PDM: a document's download/text-extraction is the expensive,
    failure-prone step worth not repeating, regardless of whether it
    previously succeeded or failed.
    """
    already_processed = {
        str(document.url): existing
        for document in documents
        if (existing := output_store.get_document(str(document.url))) is not None
    }
    to_process = [document for document in documents if str(document.url) not in already_processed]

    newly_extracted = (
        cast(
            "list[RegulationDocument]",
            extract_pdf_text_task.map(
                unmapped(tmp_dir),
                to_process,
                unmapped(pdf_download_timeout_seconds),
            ).result(),
        )
        if to_process
        else []
    )

    by_url = {**already_processed, **{str(document.url): document for document in newly_extracted}}
    return [by_url[str(document.url)] for document in documents]


async def process_municipio(
    session: AsyncStealthySession,
    tmp_dir: Path,
    municipio: str,
    pdf_download_timeout_seconds: float,
    output_store: OutputStore,
) -> list[PDMRecord]:
    """One municipality's full pipeline: search SNIT, resolve its PDM(s)
    regulation-document history, then immediately extract those documents'
    text -- called once per municipality, fanned out by the caller (see
    extract_pdms.ExtractPDM.extract_pdms), so this municipality's PDF
    extraction runs concurrently with SNIT lookups still in flight for
    others rather than waiting for every municipality to be resolved first.

    Search and fetch always re-run (they're cheap metadata lookups), but
    each resolved document's download/text-extraction is skipped when
    `output_store` already has a result for it -- see _extract_documents.

    Both SNIT calls retry on failure (see their tasks' `retries`), since
    the portal occasionally serves a broken response under load; once
    retries are exhausted here, that's treated as "found nothing" for
    search or "this PDM couldn't be resolved" for fetch, logged rather than
    aborting the whole flow.
    """
    logger = get_run_logger()

    try:
        records = await search_municipio_task(session, municipio)
    except Exception:
        logger.exception(f"{municipio}: SNIT search failed after retries")
        records = []

    pdm_series = [
        record
        for record in records
        if record["Type"] == "series"
        and record["Title"].startswith(SnitSearch.PDM_TITLE_PREFIX)
    ]

    results: list[PDMRecord] = []

    for record in pdm_series:
        try:
            pdm_record = await fetch_regulation_documents_task(session, municipio, record)
        except Exception:
            logger.exception(f"{municipio}: regulation lookup failed for {record['Title']} after retries")
            continue

        if not pdm_record.documents:
            results.append(pdm_record)
            continue

        extracted = await _extract_documents(
            tmp_dir, pdm_record.documents, pdf_download_timeout_seconds, output_store
        )
        results.append(pdm_record.model_copy(update={"documents": extracted}))

    return results
