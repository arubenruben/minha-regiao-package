from contextlib import asynccontextmanager
from typing import AsyncIterator

from tortoise import Tortoise

MODEL_MODULES = [
    "minha_regiao.entity",
    "minha_regiao.entity.elections",
]


@asynccontextmanager
async def connection(db_url: str) -> AsyncIterator[None]:
    await Tortoise.init(db_url=db_url, modules={"models": MODEL_MODULES})
    try:
        yield
    finally:
        await Tortoise.close_connections()
