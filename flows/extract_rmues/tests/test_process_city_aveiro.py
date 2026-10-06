"""Debug harness: runs `process_city_task` for a single municipality (Aveiro)
against the real Diário da República website, so a breakpoint can be set and
the pipeline followed with real, representative data instead of running the
whole `extract_rmues` flow over every municipality.

This is NOT a unit test. It opens a headless browser (scrapling's
StealthyFetcher/AsyncStealthySession) and downloads real PDFs, so it needs
network access and scrapling's browser dependencies installed, and it takes
well over a minute. It writes to a temporary directory only, never to the
real `out/`, and forces `load_targets = ["json"]`, so no Postgres is needed.

Run it from `flows/extract_rmues`, either as a script or through pytest
(`-s` so the per-document summary is printed):

    uv run --package extract_rmues python tests/test_process_city_aveiro.py
    uv run --package extract_rmues pytest tests/test_process_city_aveiro.py -s
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch

from prefect import flow

from extract_rmues.schema.RMUERegulation import DocumentStatus, RMUERegulation
from extract_rmues.services.OutputStore import OutputStore
from extract_rmues.Settings import settings
from extract_rmues.tasks.FetchRmuePage import fetch_rmue_page_task
from extract_rmues.tasks.ParseRmuePage import parse_rmue_page_task
from extract_rmues.tasks.ProcessCity import process_city_task

MUNICIPALITY = "Aveiro"


@flow(name="test_process_city")
async def _process_city_flow(entry: RMUERegulation, output_store: OutputStore) -> RMUERegulation:
    # process_city_task `.map()`s resolve_pdf_url_task/extract_notice_text_task,
    # which need a flow run context to pick their task runner -- hence this
    # wrapper flow rather than awaiting the task bare.
    return await process_city_task(entry, output_store, {})


async def test_process_city_aveiro() -> None:
    page = await fetch_rmue_page_task(settings.rmue_url)
    entries = await parse_rmue_page_task(page)

    entry = next((entry for entry in entries if entry.municipality == MUNICIPALITY), None)
    assert entry is not None, (
        f"Municipality {MUNICIPALITY!r} not found on {settings.rmue_url}; available municipalities: "
        f"{sorted(entry.municipality for entry in entries)}"
    )

    with (
        patch.object(settings, "load_targets", ["json"]),
        tempfile.TemporaryDirectory(prefix="extract_rmues_test_") as tmp_dir_name,
    ):
        output_store = OutputStore(Path(tmp_dir_name) / "rmues.json")
        resolved_entry = await _process_city_flow(entry, output_store)

    documents = [*resolved_entry.urbanization_documents, *resolved_entry.fee_documents]

    for document in documents:
        print(f"{document.name} | {document.status.value} | {document.pdf_url}")

    pending = [document.name for document in documents if document.status == DocumentStatus.PENDING]
    assert not pending, f"Documents left with status=PENDING: {pending}"


if __name__ == "__main__":
    asyncio.run(test_process_city_aveiro())
