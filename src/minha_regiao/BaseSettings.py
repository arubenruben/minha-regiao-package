from abc import ABC
from environs import Env
from pydantic_settings import BaseSettings as PydanticBaseSettings, SettingsConfigDict

class BaseSettings(PydanticBaseSettings, ABC):
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8"
    )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        Env().read_env(override=True)