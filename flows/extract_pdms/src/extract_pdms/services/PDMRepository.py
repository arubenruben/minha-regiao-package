import logging

from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.entity.PDM import PDM
from minha_regiao.utils.FuzzyMatch import resolve_name

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import DocumentStatus, RegulationDocument
from extract_pdms.services.SnitSearch import SNIT_CATALOGUE_ID, SNIT_PORTAL_URL

logger = logging.getLogger(__name__)


def _select_latest_document(documents: list[RegulationDocument]) -> RegulationDocument | None:
    """Picks the most recent successfully-extracted document to represent a
    PDM row's `pdf_url` -- the `PDM` table has exactly one `pdf_url` per
    city (`PDM.city` is `unique=True`), while a `PDMRecord` can carry
    several regulation documents (revisions/corrections/amendments) across
    its history. Ranked by year, then by suffix (a document's
    re-published/corrected copy number within the same year) descending.
    Returns None if none of `documents` extracted successfully.
    """
    ok_documents = [document for document in documents if document.status == DocumentStatus.OK]
    if not ok_documents:
        return None

    return max(ok_documents, key=lambda document: (document.year, document.suffix or 0))


def _build_source_url(identifier: str) -> str:
    """SNIT has no standalone per-PDM detail page -- every lookup
    (`SnitSearch.search_municipio`, `fetch_pdm_pdf_urls`) is a POST against
    the same portal page keyed by `identifier`/catalogue. There is no
    existing "build this as a browsable URL" helper to reuse, so this
    mirrors that lookup as a query string against the portal page, which
    at least points a human at where the PDM was resolved from.
    """
    return f"{SNIT_PORTAL_URL}?catalogue={SNIT_CATALOGUE_ID}&identifier={identifier}"


async def persist_pdms(db_url: str, records: list[PDMRecord]) -> int:
    """Matches each `PDMRecord`'s municipality against `City` (fuzzy, same
    as `extract_rmues.services.RMURepository.persist_rmue_regulations`) and
    upserts one `PDM` row per city, using the most recent
    successfully-extracted regulation document as `pdf_url`. Records that
    don't match a city, or have no successfully-extracted document, are
    skipped and logged rather than aborting the whole batch.
    """
    async with connection(db_url):
        cities_by_name = {city.name: city async for city in City.all()}

        persisted = 0
        unmatched_cities: list[str] = []
        skipped_no_document: list[str] = []

        for record in records:
            matched_name = resolve_name(record.municipio, cities_by_name)
            if matched_name is None:
                unmatched_cities.append(record.municipio)
                continue

            document = _select_latest_document(record.documents)
            if document is None:
                skipped_no_document.append(record.municipio)
                continue

            city = cities_by_name[matched_name]

            await PDM.update_or_create(
                city=city,
                defaults={
                    "source_url": _build_source_url(record.identifier),
                    "pdf_url": str(document.url),
                },
            )
            persisted += 1

    if unmatched_cities:
        logger.warning(f"Unmatched municipalities: {sorted(unmatched_cities)}")
    if skipped_no_document:
        logger.warning(f"No successfully-extracted document for: {sorted(skipped_no_document)}")

    return persisted
