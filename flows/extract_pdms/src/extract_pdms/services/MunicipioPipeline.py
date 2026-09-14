from pathlib import Path
from typing import cast

from prefect import unmapped
from scrapling.fetchers import AsyncStealthySession

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument
from extract_pdms.services import SnitSearch
from extract_pdms.tasks.ExtractPdfText import extract_pdf_text_task
from extract_pdms.tasks.FetchRegulationDocuments import fetch_regulation_documents_task
from extract_pdms.tasks.SearchMunicipio import search_municipio_task


async def process_municipio(
    session: AsyncStealthySession,
    tmp_dir: Path,
    municipio: str,
    pdf_download_timeout_seconds: float,
) -> list[PDMRecord]:
    """One municipality's full pipeline: search SNIT, resolve its PDM(s)
    regulation-document history, then immediately extract those documents'
    text -- called once per municipality, fanned out by the caller (see
    extract_pdms.ExtractPDM.extract_pdms), so this municipality's PDF
    extraction runs concurrently with SNIT lookups still in flight for
    others rather than waiting for every municipality to be resolved first.
    """
    records = await search_municipio_task(session, municipio)

    pdm_series = [
        record
        for record in records
        if record["Type"] == "series"
        and record["Title"].startswith(SnitSearch.PDM_TITLE_PREFIX)
    ]

    results: list[PDMRecord] = []

    for record in pdm_series:
        pdm_record = await fetch_regulation_documents_task(session, municipio, record)
        if pdm_record is None:
            continue

        if not pdm_record.documents:
            results.append(pdm_record)
            continue

        extracted = cast(
            "list[RegulationDocument]",
            extract_pdf_text_task.map(
                unmapped(tmp_dir),
                pdm_record.documents,
                unmapped(pdf_download_timeout_seconds),
            ).result(),
        )
        results.append(pdm_record.model_copy(update={"documents": extracted}))

    return results
