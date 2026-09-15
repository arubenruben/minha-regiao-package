from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Scaffold only -- this sub-flow is currently an unimplemented stub
    (see TownHallElections.py). `load_targets` is prepared here so whoever
    implements the actual parsing/persistence logic wires it onto
    `minha_regiao.loader` from day one, following the pattern used in
    extract_pdms/extract_rmues (see flows/CLAUDE.md).
    """

    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    load_targets: list[Literal["database", "json"]] = ["json"]


settings = Settings()
