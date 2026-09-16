import threading
import time
from pathlib import Path

from extract_rmues.schema.RMUEExtractionState import RMUEExtractionState
from extract_rmues.schema.RMUERegulation import RegulationDocument, RMUERegulation

# On Windows, replacing a file that's momentarily open elsewhere (an AV
# scanner, a search indexer, or -- most likely here, since this runs under
# a OneDrive-synced Desktop -- the OneDrive sync client noticing the write)
# raises PermissionError (WinError 5) even though the lock clears within
# milliseconds. Retried with backoff rather than failing the run or giving
# up the atomic write (which is what makes a crash mid-write safe). Mirrors
# extract_pdms.services.OutputStore.
_REPLACE_RETRY_DELAYS_SECONDS = (0.05, 0.1, 0.2, 0.4, 0.8)


class OutputStore:
    """Persists extract_rmues' progress to `path`, so a crashed or
    interrupted run resumes from where it left off instead of repeating
    the expensive, failure-prone step: resolving a document's PDF url and
    extracting its own notice text/structure. Idempotence is tracked per
    document (by dre_url) via `get_document`, mirroring
    extract_pdms.services.OutputStore -- not per municipality, since a
    municipality's own document set can grow across runs (see
    `get_entry`/`record`).

    `record()` is the flow's critical section and its only mutator: it's
    called once per municipality, concurrently, from every mapped task run
    (see extract_rmues.tasks.ProcessCity), so the read-modify-write of the
    shared state and its persistence to disk are both guarded by `_lock`,
    and the write itself is atomic (write-then-replace) so a crash
    mid-write can't corrupt a previous run's progress.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()

        state = self._read()
        self._records: dict[str, RMUERegulation] = {entry.municipality: entry for entry in state.entries}
        self._documents_by_url: dict[str, RegulationDocument] = {
            document.dre_url: document
            for entry in state.entries
            for document in (*entry.urbanization_documents, *entry.fee_documents)
        }

    def _read(self) -> RMUEExtractionState:
        if not self._path.exists():
            return RMUEExtractionState()
        return RMUEExtractionState.model_validate_json(self._path.read_text(encoding="utf-8"))

    def _write(self) -> None:
        state = RMUEExtractionState(entries=list(self._records.values()))

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

    def get_document(self, dre_url: str) -> RegulationDocument | None:
        """Returns a previously recorded document by dre_url, if any --
        its PDF-url resolution/extraction should be skipped and this
        result reused instead, regardless of whether it succeeded."""
        with self._lock:
            return self._documents_by_url.get(dre_url)

    def get_entry(self, municipality: str) -> RMUERegulation | None:
        """Returns this municipality's previously recorded entry, if any --
        used by extract_rmues.tasks.ProcessCity to merge this run's (often
        partial, e.g. only documents still missing a PDF url in Postgres)
        results into the full document set already on record, rather than
        overwriting it and losing documents this run didn't touch."""
        with self._lock:
            return self._records.get(municipality)

    def record(self, entry: RMUERegulation) -> None:
        """Upserts `entry` -- keyed by municipality, so re-processing the
        same city replaces its previous entry rather than duplicating it --
        and indexes its documents by dre_url, persisting the combined state
        before returning.
        """
        with self._lock:
            self._records[entry.municipality] = entry
            for document in (*entry.urbanization_documents, *entry.fee_documents):
                self._documents_by_url[document.dre_url] = document
            self._write()

    @property
    def records(self) -> list[RMUERegulation]:
        with self._lock:
            return list(self._records.values())
