import threading
from pathlib import Path

from minha_regiao.loader.JsonFileLoader import JsonFileLoader
from pydantic import TypeAdapter

from extract_rmues.schema.RMUERegulation import RegulationDocument, RMUERegulation

_ENTRIES_ADAPTER = TypeAdapter(list[RMUERegulation])


class OutputStore:
    """The JSON sink for extract_rmues and, at the same time, the source of
    idempotence of its JSON mode: `path` is the flow's output file -- a plain
    list of `RMUERegulation`, the shape `JsonFileLoader` writes -- which is
    read back when the store is built, so a crashed or interrupted run
    resumes from where it left off instead of repeating the expensive,
    failure-prone step: resolving a document's PDF url and extracting its
    own notice text/structure. Idempotence is tracked per document (by
    dre_url) via `get_document`, mirroring extract_pdms.services.OutputStore
    -- not per municipality, since a municipality's own document set can
    grow across runs.

    `record()` is the flow's critical section and its only mutator: it's
    called once per municipality, concurrently, from every mapped task run
    (see extract_rmues.tasks.ProcessCity) -- each on its own thread and
    event loop -- and rewrites the whole file via `JsonFileLoader`
    (atomically, write-then-replace, so a crash mid-write can't corrupt a
    previous run's progress). The upsert and that rewrite both happen under
    `_lock`, held across the awaited write, so two municipalities finishing
    at the same time can't write the file out of order. Holding a
    `threading.Lock` across an `await` is only safe because no two callers
    share an event loop: one blocked on the lock would otherwise stop the
    loop its holder needs to resume on.
    """

    def __init__(self, path: Path) -> None:
        self._loader = JsonFileLoader[RMUERegulation](path)
        self._lock = threading.Lock()

        entries = _ENTRIES_ADAPTER.validate_json(path.read_text(encoding="utf-8")) if path.exists() else []

        self._records: dict[str, RMUERegulation] = {entry.municipality: entry for entry in entries}
        self._documents_by_url: dict[str, RegulationDocument] = {
            document.dre_url: document
            for entry in entries
            for document in (*entry.urbanization_documents, *entry.fee_documents)
        }

    def get_document(self, dre_url: str) -> RegulationDocument | None:
        """Returns a previously recorded document by dre_url, if any --
        its PDF-url resolution/extraction should be skipped and this
        result reused instead, regardless of whether it succeeded."""
        with self._lock:
            return self._documents_by_url.get(dre_url)

    async def record(self, entry: RMUERegulation) -> None:
        """Upserts `entry` -- keyed by municipality, so re-processing the
        same city replaces its previous entry rather than duplicating it --
        indexes its documents by dre_url, and rewrites the output file with
        every entry held so far before returning.
        """
        with self._lock:
            self._records[entry.municipality] = entry
            for document in (*entry.urbanization_documents, *entry.fee_documents):
                self._documents_by_url[document.dre_url] = document
            await self._loader.load(list(self._records.values()))
