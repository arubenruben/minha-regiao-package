from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    rmue_url: str = "https://diariodarepublica.pt/dr/geral/areas-tematicas/regul-municipais"

    # How many municipalities have their documents resolved/extracted in
    # parallel (see extract_rmues.tasks.ProcessCity). Independent of
    # pdf_resolve_concurrency/pdf_extract_concurrency below, which cap the
    # total number of in-flight resolutions/extractions across every
    # municipality being processed at once -- mirrors extract_pdms's
    # snit_concurrency (outer, per-municipality) vs
    # pdf_download_concurrency (inner, per-document) split.
    city_concurrency: int = 4

    # How many DR detail pages are resolved to their PDF url in parallel.
    # Each resolution opens its own browser (see
    # extract_rmues.tasks.ResolvePdfUrl), so this also bounds how many
    # browsers are open concurrently.
    pdf_resolve_concurrency: int = 8

    # How many resolved PDFs are downloaded and segmented to their own
    # notice text in parallel (see extract_rmues.tasks.ExtractNoticeText).
    pdf_extract_concurrency: int = 8
    pdf_extract_timeout_seconds: float = 60.0

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:5432/minha_regiao"

    # Which sinks the flow writes the enriched RMUE entries to, both per
    # municipality, as each one finishes (see extract_rmues.tasks.ProcessCity):
    # "database" and "json". Defaults to json-only, so reproducing this flow
    # never requires a database; opt into "database" explicitly. Each target
    # is also a source of idempotence for its own mode: "json" reads
    # output_file back, and "database" reads the documents already in
    # Postgres at the start of the run (see
    # extract_rmues.tasks.FindProcessedDocuments) -- either way, they aren't
    # resolved/extracted (or written) again.
    load_targets: list[Literal["database", "json"]] = ["json"]

    # Where the enriched RMUE entries (with resolved pdf_url and extracted
    # notice text/structure) are written as JSON, rewritten in full as each
    # city finishes. Only used when "json" is in load_targets. Also the JSON
    # mode's source of idempotence: it's read back at the start of the run,
    # and the documents already in it (by dre_url) aren't re-resolved or
    # re-downloaded -- see extract_rmues.services.OutputStore.
    output_file: Path = Path(__file__).with_name("out") / "rmues.json"


settings = Settings()
