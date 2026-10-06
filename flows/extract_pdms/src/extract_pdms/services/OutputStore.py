import threading
from pathlib import Path

from minha_regiao.loader.JsonFileLoader import JsonFileLoader
from pydantic import TypeAdapter

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument

_RECORDS_ADAPTER = TypeAdapter(list[PDMRecord])


class OutputStore:
    """The JSON sink for extract_pdms and, at the same time, the source of
    idempotence of its JSON mode: `path` is the flow's output file -- a plain
    list of `PDMRecord`, the shape `JsonFileLoader` writes -- which is read
    back when the store is built, so a crashed or interrupted run resumes
    from where it left off instead of repeating the expensive, failure-prone
    step: downloading and extracting text from a regulation PDF. Idempotence
    is tracked per document (by URL), not per municipality or per PDM --
    `get_document()` is how a caller checks whether a document has already
    been processed and should be skipped, reusing the recorded result
    instead.

    `record()` is the flow's critical section and its only mutator: it's
    called once per municipality, concurrently, from every mapped task run
    -- each on its own thread and event loop (see
    extract_pdms.tasks.ProcessMunicipio) -- and rewrites the whole file via
    `JsonFileLoader` (atomically, write-then-replace, so a crash mid-write
    can't corrupt a previous run's progress). The upsert and that rewrite
    both happen under `_lock`, held across the awaited write, so two
    municipalities finishing at the same time can't write the file out of
    order. Holding a `threading.Lock` across an `await` is only safe because
    no two callers share an event loop: one blocked on the lock would
    otherwise stop the loop its holder needs to resume on.
    """

    def __init__(self, path: Path) -> None:
        self._loader = JsonFileLoader[PDMRecord](path)
        self._lock = threading.Lock()

        records = _RECORDS_ADAPTER.validate_json(path.read_text(encoding="utf-8")) if path.exists() else []

        self._records: dict[tuple[str, str], PDMRecord] = {
            (record.municipio, record.identifier): record for record in records
        }
        self._documents_by_url: dict[str, RegulationDocument] = {
            str(document.url): document for record in records for document in record.documents
        }

    def get_document(self, url: str) -> RegulationDocument | None:
        """Returns a previously recorded regulation document by URL, if
        any -- its download/text-extraction should be skipped and this
        result reused instead, regardless of whether it succeeded."""
        with self._lock:
            return self._documents_by_url.get(url)

    async def record(self, municipio: str, records: list[PDMRecord]) -> None:
        """Upserts `records` -- keyed by (municipio, PDM identifier), so
        re-resolving the same PDM replaces its previous entry rather than
        duplicating it -- indexes their documents by URL, and rewrites the
        output file with every record held so far before returning.
        """
        with self._lock:
            for pdm_record in records:
                self._records[(municipio, pdm_record.identifier)] = pdm_record
                for document in pdm_record.documents:
                    self._documents_by_url[str(document.url)] = document
            await self._loader.load(list(self._records.values()))
