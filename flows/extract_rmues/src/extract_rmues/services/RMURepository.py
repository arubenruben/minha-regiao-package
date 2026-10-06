import logging

from tortoise.models import Model

from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.entity.FeeRegulation import FeeRegulation
from minha_regiao.entity.RMUE import RMUE
from minha_regiao.gazette.StructureDeduplicator import deduplicate_structure, describe_duplicates
from minha_regiao.utils.FuzzyMatch import resolve_name
from extract_rmues.schema.RMUERegulation import DocumentStatus, RegulationDocument, RMUERegulation
from extract_rmues.services.RegulationMetadata import extract_year, is_complete

logger = logging.getLogger(__name__)


def _to_regulation_document(row: RMUE | FeeRegulation) -> RegulationDocument:
    """Rebuilds a `RegulationDocument` from a persisted `RMUE`/`FeeRegulation`
    row, with every persisted field -- including `status`, `raw_text` and the
    `structure` JSON column, which pydantic validates back into
    `list[StructureNode]`.
    """
    return RegulationDocument.model_validate(
        {
            "name": row.name,
            "dre_url": row.dre_url,
            "pdf_url": row.pdf_url,
            "status": DocumentStatus(row.status.value),
            "raw_text": row.raw_text,
            "structure": row.structure,
        }
    )


async def find_processed_documents(db_url: str) -> dict[str, RegulationDocument]:
    """Reads every `RMUE` and `FeeRegulation` row back as a
    `RegulationDocument`, keyed by `dre_url` -- the database as a source of
    idempotence, for when the local `OutputStore` JSON file isn't there
    (another machine, a fresh container, a deleted file) or `"json"` isn't in
    `load_targets`. A document found here is never resolved/extracted again,
    whatever its `status`, the same as one found in the `OutputStore`.
    """
    async with connection(db_url):
        rows = [*await RMUE.all(), *await FeeRegulation.all()]

    return {row.dre_url: _to_regulation_document(row) for row in rows}


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
            f"Removed {len(result.duplicates)} duplicate article(s) from the structure of {document.dre_url}: "
            f"{describe_duplicates(result.duplicates)}"
        )

    return [node.model_dump(mode="json") for node in result.structure]


async def _persist_documents(
    model: type[Model], city: City, documents: list[RegulationDocument]
) -> tuple[int, list[str]]:
    persisted = 0
    skipped = []

    for document in documents:
        year = extract_year(document.name)
        if year is None:
            skipped.append(document.name)
            continue

        await model.update_or_create(
            city=city,
            dre_url=document.dre_url,
            defaults={
                "year": year,
                "name": document.name,
                "is_complete": is_complete(document.name),
                "pdf_url": document.pdf_url,
                "status": document.status.value,
                "raw_text": document.raw_text,
                "structure": _dump_structure(document),
            },
        )
        persisted += 1

    return persisted, skipped


async def persist_regulation_results(
    db_url: str, entries: list[RMUERegulation]
) -> tuple[int, list[str], list[str]]:
    """Matches each entry's municipality name against `City` and upserts one
    `RMUE`/`FeeRegulation` row per document, keyed by (city, dre_url), with
    the document's final state in one write: `year`/`name`/`is_complete`
    from its name, plus the `pdf_url` and extraction outcome
    (`status`/`raw_text`/`structure`, see extract_rmues.tasks.ProcessCity)
    -- so a document is never persisted half-resolved. Returns the number of
    rows written, the municipality names that couldn't be matched, and the
    names of the documents whose year couldn't be parsed (and were
    therefore skipped).

    Every document in `entries` is written: filtering out the ones already
    in the database (see `find_processed_documents`) is the caller's job --
    see extract_rmues.tasks.ProcessCity -- so each document reaches Postgres
    exactly once.
    """
    async with connection(db_url):
        cities_by_name = {city.name: city async for city in City.all()}

        persisted = 0
        unmatched_cities: list[str] = []
        skipped_documents: list[str] = []

        for entry in entries:
            matched_name = resolve_name(entry.municipality, cities_by_name)
            if matched_name is None:
                unmatched_cities.append(entry.municipality)
                continue

            city = cities_by_name[matched_name]

            rmue_persisted, rmue_skipped = await _persist_documents(RMUE, city, entry.urbanization_documents)
            fee_persisted, fee_skipped = await _persist_documents(FeeRegulation, city, entry.fee_documents)

            persisted += rmue_persisted + fee_persisted
            skipped_documents.extend(rmue_skipped + fee_skipped)

    return persisted, unmatched_cities, skipped_documents
