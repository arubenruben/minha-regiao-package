from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    district_dataset_config_name: str = "districts"

    # Which sinks the flow writes to at the end of its run. Defaults to
    # both, matching this flow's original (pre-Loader) behavior exactly.
    load_targets: list[Literal["database", "huggingface"]] = ["database", "huggingface"]


settings = Settings()
