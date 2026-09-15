from pathlib import Path

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
    # municipality has no PDM".
    snit_task_retries: int = 3
    snit_retry_delay_seconds: float = 5.0

    # How many regulation PDFs can be downloaded/parsed concurrently.
    pdf_download_concurrency: int = 8
    pdf_download_timeout_seconds: float = 60.0

    # Where the enriched PDM records (with extracted regulation text) are
    # written as JSON.
    output_file: Path = Path(__file__).with_name("out") / "pdms.json"


settings = Settings()
