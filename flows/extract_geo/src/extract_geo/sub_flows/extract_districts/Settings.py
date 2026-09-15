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
    # json-only, so reproducing this flow never requires a database or a
    # Hugging Face token; opt into "database"/"huggingface" explicitly.
    load_targets: list[Literal["database", "huggingface", "json"]] = ["json"]

    # Only used when "json" is in load_targets.
    output_file: Path = Path(__file__).with_name("out") / "districts.json"


settings = Settings()
