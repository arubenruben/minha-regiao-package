import json
import time
from pathlib import Path
from typing import Generic, TypeVar

from pydantic import BaseModel

RecordT = TypeVar("RecordT", bound=BaseModel)

# On Windows, replacing a file that's momentarily open elsewhere (an AV
# scanner, a search indexer, or a OneDrive-synced folder noticing the
# write) raises PermissionError (WinError 5) even though the lock clears
# within milliseconds. Retried with backoff rather than failing the run or
# giving up the atomic write (which is what makes a crash mid-write safe).
# Mirrors extract_pdms.services.OutputStore._write.
_REPLACE_RETRY_DELAYS_SECONDS = (0.05, 0.1, 0.2, 0.4, 0.8)


class JsonFileLoader(Generic[RecordT]):
    """Writes a batch of records to `path` as JSON, atomically (write to a
    `.tmp` sibling, then `Path.replace()`). Unlike
    `extract_pdms.services.OutputStore` -- which is an incremental,
    per-document resume cache -- this loader is a final sink: every call to
    `load()` overwrites the full file with the given batch.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    async def load(self, records: list[RecordT]) -> None:
        payload = [record.model_dump(mode="json") for record in records]

        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")

        tmp_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        for delay in (*_REPLACE_RETRY_DELAYS_SECONDS, None):
            try:
                tmp_path.replace(self._path)
                return
            except PermissionError:
                if delay is None:
                    raise
                time.sleep(delay)
