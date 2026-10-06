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
    # written as JSON, rewritten in full as each municipality finishes. Only
    # used when "json" is in load_targets. Also the JSON mode's source of
    # idempotence: it's read back at the start of the run, and the documents
    # already in it aren't downloaded/extracted again -- see
    # extract_pdms.services.OutputStore.
    output_file: Path = Path(__file__).with_name("out") / "pdms.json"

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:5432/minha_regiao"

    # Which sinks the flow writes its PDM records to, both per municipality,
    # as each one finishes (see extract_pdms.tasks.ProcessMunicipio):
    # "database" and "json". Defaults to JSON only, so reproducing this flow
    # never requires a database. Each target is also a source of idempotence
    # for its own mode: "json" reads output_file back, and "database" reads
    # the documents already in Postgres at the start of the run (see
    # extract_pdms.ExtractPDM.find_processed_documents_task) -- either way,
    # they aren't downloaded/extracted (or written) again.
    load_targets: list[Literal["database", "json"]] = ["json"]


settings = Settings()
