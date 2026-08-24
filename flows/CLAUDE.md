# Prefect flow conventions

Rules for everything under `flows/`.

## Tag-based concurrency is mandatory for fan-out

Any flow that fans out per-item task runs (one task run per city/file/page/etc.,
via `asyncio.gather`, `asyncio.as_completed`, or `.submit()`) MUST cap that
fan-out with a **Prefect tag-based concurrency limit**, not a local
`asyncio.Semaphore` alone. This applies in particular to any flow that crawls or
otherwise hits external sites — it's the main lever for avoiding
rate-limiting/403s at the source, and for not overwhelming shared resources
(DB pools, browser tab pools, GPUs, ...) when many task runs start at once.

Reference implementation: [`extract_pdms/src/extract_pdms/ExtractPDM.py`](extract_pdms/src/extract_pdms/ExtractPDM.py)
and [`extract_pdms/src/extract_pdms/services/ConcurrencyLimiter.py`](extract_pdms/src/extract_pdms/services/ConcurrencyLimiter.py).
Read `crawl_cities_for_pdm` there before implementing this in a new flow.

### The pattern — both halves are required together

1. **Tag the task**, e.g. `@task(tags=["my-flow-item"])`.

2. **Register a matching Prefect concurrency limit at the start of the flow
   that does the fan-out**, sized from a settings field (never hardcoded):

   ```python
   await ensure_concurrency_limit(MY_TAG, settings.my_concurrency)
   ```

   Reuse `ensure_concurrency_limit` from `extract_pdms/src/extract_pdms/services/ConcurrencyLimiter.py`
   (or port it into the flow's own `services/` if it can't be imported directly)
   rather than reimplementing it — it's a thin, idempotent wrapper around
   `client.create_concurrency_limit(tag=..., concurrency_limit=...)`, safe to call
   on every flow run.

3. **Gate task-run creation with an `asyncio.Semaphore` sized to the same
   number.** The Prefect limit alone only makes excess task runs *queue*
   server-side; if every task run is still created up front (e.g.
   `asyncio.gather(*[my_task(x) for x in items])`), all of them race to acquire
   a concurrency-slot lease at the same instant, which can flood Prefect's
   lease-acquisition service on a large batch — this has caused real crashes.
   The semaphore keeps the number of task runs actually in flight matched to
   the server-side limit, so leases are acquired promptly instead of piling up:

   ```python
   semaphore = asyncio.Semaphore(settings.my_concurrency)

   async def bounded(item):
       async with semaphore:
           return await my_task(item)
   ```

### Also required

- The concurrency ceiling is a `pydantic BaseSettings` field on the flow's own
  `Settings.py`, not a literal — tuning it must never require a code change.
- Don't pass live, unhashable objects (browser sessions, HTTP clients, DB
  connections) as task arguments. Prefect hashes task inputs for its default
  cache key and will raise on these. Prefer closing over them instead — define
  the task as a nested `@task` inside the flow function, capturing the shared
  object via closure (see `crawl_city` in `ExtractPDM.py`) — or, if a task
  genuinely can't avoid taking one as an argument, set `persist_result=False`
  on it rather than importing `prefect.cache_policies.NO_CACHE`.
