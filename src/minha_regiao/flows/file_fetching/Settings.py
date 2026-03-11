from environs import Env
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8"
    )

    def __init__(self) -> None:
        super().__init__()        
        Env().read_env(override=True)
    
    sg_mai_link: str
    ollama_model: str
    ollama_endpoint: str