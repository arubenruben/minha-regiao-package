from environs import Env
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    def __init__(self, **kwargs):
        env = Env()
        env.read_env()
        super().__init__(**kwargs)

    smart_proxy_api_key: str

    class Config:
        env_file = ".env"