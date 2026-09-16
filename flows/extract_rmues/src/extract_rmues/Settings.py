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

    # Which sinks the flow writes the enriched RMUE entries to. Defaults to
    # json-only, so reproducing this flow never requires a database; opt
    # into "database" explicitly.
    load_targets: list[Literal["database", "json"]] = ["json"]

    # Where the enriched RMUE entries (with resolved pdf_url and extracted
    # notice text/structure) are written as JSON. Only used when "json" is
    # in load_targets.
    output_file: Path = Path(__file__).with_name("out") / "rmues.json"

    # OutputStore's resumable RMUEExtractionState, checkpointed per city as
    # the run progresses -- always written, regardless of load_targets,
    # since it's what makes a re-run skip documents already resolved/
    # extracted (by dre_url) instead of reopening a browser and
    # re-downloading their PDF. Deliberately separate from output_file:
    # that path is overwritten with a plain JSON list (see JsonFileLoader)
    # once the run finishes, which isn't the shape OutputStore reads back
    # on resume.
    state_file: Path = Path(__file__).with_name("out") / "rmue_state.json"


settings = Settings()
