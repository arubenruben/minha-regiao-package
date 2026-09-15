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

## Composable `Loader` abstraction for every flow's output step

Any flow's final "write the results somewhere" step (database persistence, a
local JSON file, a Hugging Face dataset publish, ...) MUST be expressed as one
or more `Loader`s from the shared `minha_regiao.loader` package, selected at
runtime by a `load_targets` setting — not a hardcoded, unconditional call to a
publish/persist function in the flow body.

Reference implementations: [`minha_regiao/src/minha_regiao/loader/`](../minha_regiao/src/minha_regiao/loader/)
(`Loader.py`, `DatabaseLoader.py`, `JsonFileLoader.py`, `HuggingFaceLoader.py`)
and their wiring into [`extract_pdms/src/extract_pdms/ExtractPDM.py`](extract_pdms/src/extract_pdms/ExtractPDM.py)
(additive: this flow has no database persistence otherwise) and
[`extract_rmues/src/extract_rmues/ExtractRMUEs.py`](extract_rmues/src/extract_rmues/ExtractRMUEs.py)
(existing database sink rewired, plus an opt-in JSON dry-run target).

### The pattern

1. **`Loader` is a minimal `Protocol`**, generic over a pydantic record type,
   with one method: `async def load(self, records: list[RecordT]) -> None`.
   It is deliberately not a generic ORM mapper.

2. **Each sink is a thin adapter, not new business logic**:
   - `DatabaseLoader` wraps a flow's own, already-existing repository
     function (e.g. `CityRepository.persist_cities`,
     `DistrictRepository.persist_districts`, `ParishRepository.persist_parishes`,
     `RMURepository.persist_rmue_regulations`, `PDMRepository.persist_pdms`) —
     the entity-specific `update_or_create` mapping stays in that function.
   - `JsonFileLoader` writes `[r.model_dump(mode="json") for r in records]` to
     a `Path`, atomically (write-then-replace with retry-on-`PermissionError`,
     the same mechanics proven in `extract_pdms.services.OutputStore`).
   - `HuggingFaceLoader` wraps `datasets.Dataset.push_to_hub` — this is the
     canonical home for what used to be `extract_geo.services.DatasetPublisher`
     (kept as a re-export for backward compatibility).

3. **`load_targets` is a `pydantic BaseSettings` field on the flow's (or
   sub-flow's) own `Settings.py`**, typed as `list[Literal[...]]` over the
   sink names that flow supports, e.g.:

   ```python
   load_targets: list[Literal["database", "json"]] = ["json"]
   ```

   **`"json"` MUST be a valid target for every flow, and MUST be the
   default** (`load_targets` defaults to `["json"]` alone, not combined with
   `"database"`/`"huggingface"`). This is what makes every flow reproducible
   out of the box, with no database or external credentials required —
   `database`/`huggingface` are opt-in additions on top of that, never the
   default.

4. **Dispatch to each configured loader as its own Prefect task**, so one
   sink failing (e.g. disk full for the JSON write) doesn't hide a DB write
   failure or vice versa, and each gets its own retry behavior:

   ```python
   if "database" in settings.load_targets:
       await persist_my_records_task(records)
   if "json" in settings.load_targets:
       await write_my_records_json_task(records)
   ```

### Also required

- A step that's a required part of the pipeline's own logic — not a sink for
  its results — stays unconditional. For example `extract_districts`'
  `assign_city_districts` (assigning cities to districts) and
  `extract_parishes`' `load_cities_task` (reading cities as required input)
  are never gated by `load_targets`.
- A flow whose sinks fan out to *different* derived record shapes (e.g.
  `extract_cities` persists `CityContacts` to the database but publishes
  `CityDatasetRecord` to Hugging Face) gates each sink independently by its
  own `load_targets` membership check — it is not one shared record list fed
  to every loader. When `"json"` and `"huggingface"` both need the same
  derived shape (as in `extract_cities`/`extract_districts`/`extract_parishes`),
  build that shape once behind `"huggingface" in load_targets or "json" in
  load_targets`, then gate the publish call and the JSON write independently
  inside that block — don't build it twice.
- An unimplemented flow (see `parse_election_files`'s stub sub-flows) still
  gets the `load_targets` setting scaffolded on its `Settings.py` ahead of its
  parsing logic, so whoever implements it wires the output step onto
  `minha_regiao.loader` from day one instead of inventing another one-off
  publish call.
