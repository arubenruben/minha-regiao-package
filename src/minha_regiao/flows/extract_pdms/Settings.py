from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao"

    max_pages_per_site: int = 5_000
    max_depth: int = 4

    # How many cities (distinct domains) to crawl at once. Safe to raise
    # since each domain is rate-limited independently of the others.
    city_concurrency: int = 8

    # How many pages to fetch concurrently within a single site's crawl.
    # Left at 1 (fully sequential) by default since hitting one domain with
    # concurrent requests is what triggers rate limiting/403s in the first
    # place; raise deliberately, per site, once a site is known to tolerate it.
    site_concurrency: int = 1

    # Minimum delay before each request to a given site, to avoid tripping
    # rate limits.
    site_request_delay_seconds: float = 1.0

    # A 403 is treated as rate limiting rather than a hard failure: retried
    # with exponential backoff (retry_backoff_seconds * 2**attempt) instead
    # of being given up on immediately.
    max_retries_on_403: int = 3
    retry_backoff_seconds: float = 5.0


settings = Settings()
