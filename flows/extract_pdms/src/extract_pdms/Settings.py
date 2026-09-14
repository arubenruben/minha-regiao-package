from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # How many SNIT browser calls (municipality search + per-record
    # regulamento lookup) can be in flight at once. Both kinds of call share
    # one AsyncStealthySession, so this also sizes that session's tab pool.
    snit_concurrency: int = 4

    # How many regulation PDFs can be downloaded/parsed concurrently.
    pdf_download_concurrency: int = 8
    pdf_download_timeout_seconds: float = 60.0

    # Where the enriched PDM records (with extracted regulation text) are
    # written as JSON.
    output_file: Path = Path(__file__).with_name("out") / "pdms.json"


settings = Settings()
