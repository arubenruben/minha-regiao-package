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

    # How many DR detail pages are resolved to their PDF url in parallel.
    # Each resolution opens its own browser (see
    # extract_rmues.tasks.ResolvePdfUrl), so this also bounds how many
    # browsers are open concurrently.
    pdf_resolve_concurrency: int = 8

    # How many resolved PDFs are downloaded and segmented to their own
    # notice text in parallel (see extract_rmues.tasks.ExtractNoticeText).
    pdf_extract_concurrency: int = 8
    pdf_extract_timeout_seconds: float = 60.0

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao"

    # Only used when "database" isn't in load_targets: persists resolved PDF
    # urls across runs (see extract_rmues.services.ResolutionStore) so a
    # database-free run doesn't reopen a browser for a document already
    # resolved by a previous run. Ignored otherwise, since Postgres is
    # already that list's source of truth then.
    resolution_state_file: Path = Path(__file__).with_name("out") / "rmue_resolved_documents.json"

    # Which sinks the flow writes the parsed RMUE entries to. Defaults to
    # json-only, so reproducing this flow never requires a database; opt
    # into "database" explicitly.
    load_targets: list[Literal["database", "json"]] = ["json"]

    # Only used when "json" is in load_targets.
    output_file: Path = Path(__file__).with_name("out") / "rmues.json"


settings = Settings()
