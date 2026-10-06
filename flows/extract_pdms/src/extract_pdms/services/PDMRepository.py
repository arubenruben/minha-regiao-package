import logging

from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.entity.PDM import PDM, PDMDocument
from minha_regiao.gazette.StructureDeduplicator import deduplicate_structure, describe_duplicates
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


def _dump_structure(document: RegulationDocument) -> list[dict] | None:
    """`parse_structure` already deduplicates, but a structure read back from
    an earlier run's JSON output was parsed before that existed and may still
    carry the same article twice -- so it's deduplicated again here, right
    before it's persisted (a no-op on an already-clean tree).
    """
    if not document.structure:
        return None

    result = deduplicate_structure(document.structure)
    if result.duplicates:
        logger.warning(
            f"Removed {len(result.duplicates)} duplicate article(s) from the structure of {document.url}: "
            f"{describe_duplicates(result.duplicates)}"
        )

    return [node.model_dump(mode="json") for node in result.structure]


def _to_regulation_document(row: PDMDocument) -> RegulationDocument:
    """Rebuilds a `RegulationDocument` from a persisted `PDMDocument` row,
    with every persisted field -- including `status`, `text` and the
    `structure` JSON column, which pydantic validates back into
    `list[StructureNode]`.
    """
    return RegulationDocument.model_validate(
        {
            "url": row.url,
            "doc_type": row.doc_type,
            "number": row.number,
            "year": row.year,
            "suffix": row.suffix,
            "data_publicacao": row.data_publicacao,
            "dinamica": row.dinamica,
            "publicacao": row.publicacao,
            "status": DocumentStatus(row.status.value),
            "text": row.text,
            "structure": row.structure,
        }
    )


async def find_processed_documents(db_url: str) -> dict[str, RegulationDocument]:
    """Reads every `PDMDocument` row back as a `RegulationDocument`, keyed by
    its url -- the database as a source of idempotence, for when the local
    `OutputStore` state file isn't there (another machine, a fresh container,
    a deleted file). A document found here is never downloaded/extracted
    again, whatever its `status`, the same as one found in the `OutputStore`.
    """
    async with connection(db_url):
        rows = await PDMDocument.all()

    documents = (_to_regulation_document(row) for row in rows)
    return {str(document.url): document for document in documents}


async def _persist_documents(pdm: PDM, documents: list[RegulationDocument]) -> None:
    for document in documents:
        await PDMDocument.update_or_create(
            pdm=pdm,
            url=str(document.url),
            defaults={
                "doc_type": document.doc_type,
                "number": document.number,
                "year": document.year,
                "suffix": document.suffix,
                "data_publicacao": document.data_publicacao,
                "dinamica": document.dinamica,
                "publicacao": document.publicacao,
                "status": document.status.value,
                "text": document.text,
                "structure": _dump_structure(document),
            },
        )


async def persist_pdms(
    db_url: str, records: list[PDMRecord], processed_by_url: dict[str, RegulationDocument]
) -> int:
    """Matches each `PDMRecord`'s municipality against `City` (fuzzy, same
    as `extract_rmues.services.RMURepository.persist_regulation_results`) and
    upserts one `PDM` row per city -- `title`/`identifier` straight off the
    record, `pdf_url` set to the most recent successfully-extracted
    regulation document's url (or left null if none extracted
    successfully yet), both computed over *all* of `record.documents` --
    plus one `PDMDocument` row per entry in `record.documents`, capturing
    every revision's own extraction outcome (`status`/`text`/`structure`),
    not just the latest one. Records that don't match a city are skipped
    and logged rather than aborting the whole batch.

    A document already in `processed_by_url` (see `find_processed_documents`)
    is not written again, even if this run reused it from the `OutputStore`
    rather than from the database: each document is written to Postgres
    exactly once, on its first complete run.
    """
    async with connection(db_url):
        cities_by_name = {city.name: city async for city in City.all()}

        persisted = 0
        unmatched_cities: list[str] = []

        for record in records:
            matched_name = resolve_name(record.municipio, cities_by_name)
            if matched_name is None:
                unmatched_cities.append(record.municipio)
                continue

            city = cities_by_name[matched_name]
            latest_document = _select_latest_document(record.documents)

            pdm, _ = await PDM.update_or_create(
                city=city,
                defaults={
                    "title": record.title,
                    "identifier": record.identifier,
                    "source_url": _build_source_url(record.identifier),
                    "pdf_url": str(latest_document.url) if latest_document else None,
                },
            )
            new_documents = [document for document in record.documents if str(document.url) not in processed_by_url]
            await _persist_documents(pdm, new_documents)
            persisted += 1

    if unmatched_cities:
        logger.warning(f"Unmatched municipalities: {sorted(unmatched_cities)}")

    return persisted
