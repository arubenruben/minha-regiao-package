import threading
import time
from pathlib import Path

from extract_pdms.schema.ExtractionState import ExtractionState
from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.schema.RegulationDocument import RegulationDocument

# On Windows, replacing a file that's momentarily open elsewhere (an AV
# scanner, a search indexer, or -- most likely here, since this runs under
# a OneDrive-synced Desktop -- the OneDrive sync client noticing the write)
# raises PermissionError (WinError 5) even though the lock clears within
# milliseconds. Retried with backoff rather than failing the run or giving
# up the atomic write (which is what makes a crash mid-write safe).
_REPLACE_RETRY_DELAYS_SECONDS = (0.05, 0.1, 0.2, 0.4, 0.8)


class OutputStore:
    """Persists extract_pdms' progress to `path`, so a crashed or
    interrupted run resumes from where it left off instead of repeating
    the expensive, failure-prone step: downloading and extracting text
    from a regulation PDF. Idempotence is tracked per document (by URL),
    not per municipality or per PDM -- `get_document()` is how a caller
    checks whether a document has already been processed and should be
    skipped, reusing the recorded result instead.

    `record()` is the flow's critical section and its only mutator: it's
    called once per municipality, concurrently, from every mapped task run
    -- each on its own thread (see extract_pdms.tasks.ProcessMunicipio) --
    so the read-modify-write of the shared state and its persistence to
    disk are both guarded by `_lock`, and the write itself is atomic
    (write-then-replace) so a crash mid-write can't corrupt a previous
    run's progress.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()

        state = self._read()
        self._records: dict[tuple[str, str], PDMRecord] = {
            (record.municipio, record.identifier): record for record in state.records
        }
        self._documents_by_url: dict[str, RegulationDocument] = {
            str(document.url): document for record in state.records for document in record.documents
        }

    def _read(self) -> ExtractionState:
        if not self._path.exists():
            return ExtractionState()
        return ExtractionState.model_validate_json(self._path.read_text(encoding="utf-8"))

    def _write(self) -> None:
        state = ExtractionState(records=list(self._records.values()))

        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        tmp_path.write_text(state.model_dump_json(indent=2), encoding="utf-8")

        for delay in (*_REPLACE_RETRY_DELAYS_SECONDS, None):
            try:
                tmp_path.replace(self._path)
                return
            except PermissionError:
                if delay is None:
                    raise
                time.sleep(delay)

    def get_document(self, url: str) -> RegulationDocument | None:
        """Returns a previously recorded regulation document by URL, if
        any -- its download/text-extraction should be skipped and this
        result reused instead, regardless of whether it succeeded."""
        with self._lock:
            return self._documents_by_url.get(url)

    def record(self, municipio: str, records: list[PDMRecord]) -> None:
        """Upserts `records` -- keyed by (municipio, PDM identifier), so
        re-resolving the same PDM replaces its previous entry rather than
        duplicating it -- and indexes their documents by URL, persisting
        the combined state before returning.
        """
        with self._lock:
            for pdm_record in records:
                self._records[(municipio, pdm_record.identifier)] = pdm_record
                for document in pdm_record.documents:
                    self._documents_by_url[str(document.url)] = document
            self._write()

    @property
    def records(self) -> list[PDMRecord]:
        with self._lock:
            return list(self._records.values())
