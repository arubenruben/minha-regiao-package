from prefect.client.orchestration import get_client


async def ensure_concurrency_limit(tag: str, max_parallel: int) -> None:
    """Upserts a Prefect tag-based concurrency limit so that any task run
    tagged `tag` is capped at `max_parallel` concurrently *Running* task
    runs, enforced by the Prefect server itself rather than only within this
    process. Safe to call on every flow run: an existing limit for `tag` is
    simply updated to `max_parallel`.
    """
    async with get_client() as client:
        await client.create_concurrency_limit(tag=tag, concurrency_limit=max_parallel)
