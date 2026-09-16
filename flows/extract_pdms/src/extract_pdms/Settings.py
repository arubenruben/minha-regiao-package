from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # How many municipalities are processed in parallel. Each municipality
    # is mapped to its own task run with its own AsyncStealthySession (a
    # session can't be shared across mapped calls -- each runs on its own
    # fresh event loop), so this also bounds how many browsers are open
    # concurrently.
    snit_concurrency: int = 4

    # SNIT's portal occasionally serves a broken/anti-bot response (e.g. an
    # error page with no CSRF token) under load -- transient, so the search
    # and regulamento-lookup tasks retry rather than treating it as "this
    # municipality has no PDM". Delay is exponential (attempt N waits
    # snit_retry_delay_seconds * 2**(N-1)) with jitter, rather than a flat
    # delay, so repeated retries space out instead of hammering the portal
    # again right as it's rate-limiting/anti-bot-blocking.
    snit_task_retries: int = 3
    snit_retry_delay_seconds: float = 5.0
    snit_retry_jitter_factor: float = 1.0

    # How many regulation PDFs can be downloaded/parsed concurrently.
    pdf_download_concurrency: int = 8
    pdf_download_timeout_seconds: float = 60.0

    # Where the enriched PDM records (with extracted regulation text) are
    # written as JSON.
    output_file: Path = Path(__file__).with_name("out") / "pdms.json"

    # OutputStore's resumable ExtractionState, checkpointed per-document as
    # the run progresses. Deliberately separate from output_file: that path
    # is overwritten with a plain JSON list (see JsonFileLoader) once the
    # run finishes, which isn't the shape OutputStore reads back on resume.
    state_file: Path = Path(__file__).with_name("out") / "state.json"

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao"

    # Which sinks the flow writes its PDM records to at the end of the run.
    # Defaults to JSON only, matching this flow's original (pre-Loader)
    # behavior exactly -- extract_pdms has no database persistence today.
    load_targets: list[Literal["database", "json"]] = ["json"]


settings = Settings()
