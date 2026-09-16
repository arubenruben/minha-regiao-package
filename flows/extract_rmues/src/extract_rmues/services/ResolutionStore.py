import threading
import time
from pathlib import Path

from extract_rmues.schema.PendingDocument import PendingDocument
from extract_rmues.schema.ResolutionState import ResolutionState

# See extract_pdms.services.OutputStore for the same rationale: replacing a
# file that's momentarily open elsewhere (AV scanner, search indexer, a
# OneDrive-synced Desktop's sync client) raises PermissionError (WinError 5)
# on Windows even though the lock clears within milliseconds.
_REPLACE_RETRY_DELAYS_SECONDS = (0.05, 0.1, 0.2, 0.4, 0.8)


class ResolutionStore:
    """Persists resolved PDF urls to `path`, so a re-run of extract_rmues
    with no database configured (see extract_rmues.tasks.FindPendingDocuments)
    skips documents already resolved -- each of which opened its own browser
    session -- instead of repeating that work. Idempotence is tracked per
    document, keyed by dre_url, regardless of whether resolution previously
    found a PDF url or not.

    Only meaningful when `"database"` isn't in `load_targets`: with a
    database configured, Postgres itself is already the source of truth for
    which documents still need resolving (see
    RMURepository.find_documents_missing_pdf_url) and this store isn't used.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()

        state = self._read()
        self._documents_by_url: dict[str, PendingDocument] = {
            document.dre_url: document for document in state.documents
        }

    def _read(self) -> ResolutionState:
        if not self._path.exists():
            return ResolutionState()
        return ResolutionState.model_validate_json(self._path.read_text(encoding="utf-8"))

    def _write(self) -> None:
        state = ResolutionState(documents=list(self._documents_by_url.values()))

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

    def get(self, dre_url: str) -> PendingDocument | None:
        """Returns a previously resolved document by dre_url, if any --
        its PDF-url resolution should be skipped and this result reused."""
        with self._lock:
            return self._documents_by_url.get(dre_url)

    def record(self, documents: list[PendingDocument]) -> None:
        """Upserts `documents` -- keyed by dre_url -- persisting the
        combined state before returning."""
        with self._lock:
            for document in documents:
                self._documents_by_url[document.dre_url] = document
            self._write()
