import logging
from pathlib import Path

_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

_configured = False


def configure_file_logging(logs_dir: Path, filename: str = "extract_pdms.log") -> None:
    """Adds a file handler to the root logger so every log record -- ours
    (module-level `logging.getLogger(__name__)` calls) and Prefect's own
    (`get_run_logger()` is a thin wrapper around the standard logging
    module) -- ends up in `logs_dir/filename`, not just on the console.
    Idempotent: safe to call every time the flow module is imported.
    """
    global _configured
    if _configured:
        return

    logs_dir.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(logs_dir / filename, encoding="utf-8")
    handler.setFormatter(logging.Formatter(_FORMAT))
    handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    if root_logger.getEffectiveLevel() > logging.INFO:
        root_logger.setLevel(logging.INFO)

    _configured = True
