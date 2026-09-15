from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    seg_mai_base_url: str
    openrouter_api_key: str
    openrouter_model: str
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    google_api_key: str | None = None
    gemini_model: str | None = None
    hf_api_key: str
    hf_dataset_repo_id: str

    # Which sinks the flow writes its structured election records to.
    # Defaults to Hugging Face only, matching this flow's original
    # (pre-Loader) behavior exactly; "json" is an opt-in local alternative.
    load_targets: list[Literal["huggingface", "json"]] = ["huggingface"]

    # Only used when "json" is in load_targets.
    output_file: Path = Path(__file__).with_name("out") / "elections.json"

settings = Settings()
