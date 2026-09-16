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

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao"

    # Which sinks the flow writes the parsed RMUE entries to. Defaults to
    # json-only, so reproducing this flow never requires a database; opt
    # into "database" explicitly.
    load_targets: list[Literal["database", "json"]] = ["json"]

    # Only used when "json" is in load_targets.
    output_file: Path = Path(__file__).with_name("out") / "rmues.json"


settings = Settings()
