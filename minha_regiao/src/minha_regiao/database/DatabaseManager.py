import asyncio
import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator

from tortoise import Tortoise

MODEL_MODULES = [
    "minha_regiao.entity",
    "minha_regiao.entity.elections",
]

_connection_lock = threading.Lock()


@asynccontextmanager
async def connection(db_url: str) -> AsyncIterator[None]:
    """Open the Tortoise connection for the duration of the ``async with`` block.

    Tortoise's connection registry is process-global (keyed by the ``"default"``
    alias) and the asyncpg pool is bound to the event loop that created it. Prefect
    runs ``.map()`` task runs on ``ThreadPoolTaskRunner``, each in its own thread
    with its own event loop, so two concurrent ``connection()`` calls would
    otherwise close each other's connection mid-query or reuse a pool across event
    loops.

    The whole ``init -> use -> close`` cycle is therefore serialised by a
    process-wide lock: concurrent callers wait their turn instead of overlapping.
    It is a ``threading.Lock`` (not an ``asyncio.Lock``) because the competing
    callers live in different event loops, and it is acquired through
    ``asyncio.to_thread`` so waiting does not block the caller's event loop.

    Warning:
        The lock is not reentrant. Nesting ``connection()`` inside another
        ``connection()`` on the same thread blocks forever, because the inner call
        waits for a lock the outer call holds.
    """
    await asyncio.to_thread(_connection_lock.acquire)
    try:
        await Tortoise.init(db_url=db_url, modules={"models": MODEL_MODULES})
        try:
            yield
        finally:
            await Tortoise.close_connections()
    finally:
        _connection_lock.release()
