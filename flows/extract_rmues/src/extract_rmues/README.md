# extract_rmues

Extracts and indexes RMUE (Regulamento Municipal de Urbanização e
Edificação) and municipal fee regulations, published on the Diário da
República website, by city.

```
extract_rmues/
  ExtractRMUEs.py          the flow: parse the RMUE page, fan out one task
                            run per municipality to resolve/extract its
                            documents (and persist them, see "Persistence"
                            below)
  Settings.py / .env       source URL, database, load_targets, concurrency
  schema/                  RMUERegulation
  services/
    RMUEPageParser.py       parses the DR listing page
    RegulationMetadata.py   extracts a document's year/completeness/notice
                            metadata (doc_type/number/year) from its name
    PDFResolver.py          resolves a DR detail page to its PDF url
    RMURepository.py        persists RMUERegulation -> RMUE/FeeRegulation tables
                            (one row per document, with its final
                            pdf_url/status/raw_text/structure); also reads
                            every RMUE/FeeRegulation row back as a
                            RegulationDocument, keyed by dre_url
                            (find_processed_documents)
    OutputStore.py          the JSON sink (rewrites out/rmues.json after every
                            city) and, reading that same file back, the JSON
                            mode's per-document idempotence
    ConcurrencyLimiter.py   registers Prefect tag-based concurrency limits
  tasks/                   one @task per flow step; process_city is fanned
                            out via .map() (one task run per municipality,
                            capped by city_concurrency); within it,
                            resolve_pdf_url and extract_notice_text are
                            further tagged and fanned out over that city's
                            own documents, capped by pdf_resolve_concurrency
                            / pdf_extract_concurrency (see flows/CLAUDE.md)
  out/rmues.json           default JSON output (see load_targets below)
```

Downloading a resolved PDF, narrowing it down to its own notice text, and
parsing that notice's legal Parte/Título/Capítulo/Secção/Subsecção/Artigo
structure uses `minha_regiao.gazette` (shared with `extract_pdms`, since
these are the same kind of raw DR gazette page range) — see
`tasks/ExtractNoticeText.py`. The result -- `pdf_url`, `status`, `raw_text`,
and `structure` -- is recorded onto each document in `RMUERegulation` (see
`schema/RMUERegulation.py`), and, when `database` is in `LOAD_TARGETS`,
written to the matching `RMUE`/`FeeRegulation` row by
`tasks/PersistRegulationResults.py` (once per municipality, see "Persistence"
below) -- not just the JSON output.

An amending aviso quotes each article it changes and then republishes the
consolidated regulation, so the same Artigo can come out of the parser twice.
`parse_structure` collapses those copies (see
`minha_regiao.gazette.StructureDeduplicator`), and `services/RMURepository.py`
applies the same deduplication again before persisting `structure`, logging a
warning for what it removes -- so a structure read back from an earlier run's
JSON output, parsed before this existed, is cleaned too. Rows already
persisted with duplicates are not reprocessed or rewritten (see
"Idempotence" below).

## Prerequisites

- A running Postgres instance (see `dev.docker-compose.yml` at the repo
  root — `docker compose -f dev.docker-compose.yml up -d`) if `database` is
  in `load_targets`.
- Install the extras this flow needs: `uv sync --package extract_rmues`
  (or `--extra dev` for the whole project).

## Configuration

Settings are pydantic-settings, loaded from `extract_rmues/.env` — copy
`.env.example` to `.env` and fill in values.

| Variable        | Default                                                            | Notes                                          |
|------------------|----------------------------------------------------------------------|--------------------------------------------------|
| `RMUE_URL`        | DR municipal regulations listing page                                |                                                    |
| `CITY_CONCURRENCY` | `4`                                                                   | municipalities processed in parallel (see "Concurrency" below) |
| `PDF_RESOLVE_CONCURRENCY` | `8`                                                            | DR detail pages resolved to a PDF url in parallel across all municipalities (each opens its own browser) |
| `PDF_EXTRACT_CONCURRENCY` | `8`                                                            | resolved PDFs downloaded and segmented to their own notice text in parallel across all municipalities |
| `PDF_EXTRACT_TIMEOUT_SECONDS` | `60.0`                                                     | timeout for downloading a single PDF                                          |
| `DATABASE_URL`     | `postgres://minha_regiao:minha_regiao@localhost:5432/minha_regiao`   | used when `database` is in `LOAD_TARGETS` (set `POSTGRES_PORT` at the repo root if 5432 is taken) |
| `LOAD_TARGETS`     | `["json"]`                                                            | `database`, `json`, or both, each per municipality as it finishes — see [flows/CLAUDE.md](../../../CLAUDE.md) |
| `OUTPUT_FILE`      | `extract_rmues/out/rmues.json`                                       | used when `json` is in `LOAD_TARGETS`; rewritten after every city and read back at the start of the run as the JSON mode's idempotence — see "Idempotence" below |

There is no `STATE_FILE` any more: `OUTPUT_FILE` is both the output and the
checkpoint. A `rmue_state.json` left over from an earlier version is no longer
read and can be deleted.

## Running

```bash
uv run --package extract_rmues python -m extract_rmues.ExtractRMUEs

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run extract-rmues
```

The flow has a single path, independent of `load_targets`: the documents it
processes always come from the listing page parsed in the current run
(`parse_rmue_page_task`); the database only supplies results to reuse for
documents already processed (see "Idempotence"), never which documents to
process. It maps
`process_city_task` (see `tasks/ProcessCity.py`) directly over the parsed
entries, one task run per municipality. Each run resolves the PDF url of
that city's `urbanization_documents` and `fee_documents` (separately) and
extracts their own notice text/structure, and returns the city's *complete*
set of documents -- a document whose PDF url couldn't be resolved comes
back with `status=pdf_url_not_found`.

## Persistence

Both sinks are written **incrementally, by municipality**, not once at the end
of the run. The last steps of `process_city_task` write its own city: only once
*all* of that city's `urbanization_documents` and `fee_documents` have reached
their final state does it call `persist_regulation_results_task([entry])`
(`tasks/PersistRegulationResults.py`) with just that city and then
`OutputStore.record(entry)`. With `database` in `LOAD_TARGETS`, the persist
upserts one `RMUE`/`FeeRegulation` row per document, keyed by `(city, dre_url)`,
with its `year`/`name`/`is_complete` and its final
`pdf_url`/`status`/`raw_text`/`structure`. So a document reaches Postgres
exactly once, already complete -- never part-way through its processing, and a
run that fails or is interrupted midway keeps everything the cities already
finished instead of losing hours of resolution/extraction work.

- The database persist only happens when `database` is in `LOAD_TARGETS`.
- The entry passed has only the documents not already in `processed_by_url`
  (see "Idempotence"), and the task isn't called at all when that leaves
  nothing to write.
- `OutputStore` is only built when `json` is in `LOAD_TARGETS`; otherwise the
  tasks get `None` and skip it. `OutputStore.record` upserts the city (keyed by
  `municipality`) and rewrites the whole `OUTPUT_FILE` through `JsonFileLoader`
  (atomically), under a lock, so two cities finishing at the same time can't
  write it out of order. It's the last step, after the database persist: a city
  only counts as processed for the JSON mode once every other configured sink
  has it too.
- Several cities persist at the same time, from different threads;
  `minha_regiao.database.DatabaseManager.connection` serialises their Tortoise
  connections.
- If a persist fails, that city's task run fails (and the flow with it) before
  it reaches `OutputStore.record`, so it isn't in `OUTPUT_FILE` either and is
  processed again on the next run (unless its documents were already in the
  database).
- The flow itself only waits for the task runs and logs how many PDF urls were
  resolved.

The flow needs no database at all with the default `["json"]`. Municipalities
that don't match a `City`, and documents skipped because no year could be
parsed from their name, are logged as warnings.

## Concurrency

Two tiers, mirroring `extract_pdms`' municipality-outer/document-inner
split (see [flows/CLAUDE.md](../../../CLAUDE.md)):

- `CITY_CONCURRENCY` municipalities have their documents processed in
  parallel (`process_city_task`, tagged `rmue-city-pipeline`).
- Within that, `PDF_RESOLVE_CONCURRENCY`/`PDF_EXTRACT_CONCURRENCY` cap the
  total number of PDF resolutions/extractions in flight *across every
  municipality being processed at once*, not per city — so raising
  `CITY_CONCURRENCY` alone doesn't multiply how many browsers/downloads run
  concurrently.

## Idempotence

Resolving a PDF url opens its own headless browser session per document,
and extracting its notice text downloads and parses a PDF — both expensive
and worth skipping on a re-run:

Idempotence is per document, keyed by `dre_url`, regardless of whether it
succeeded or failed. There are two sources for "already has a result",
checked in this order:

- `OutputStore` (`services/OutputStore.py`), only when `json` is in
  `load_targets`. It is the JSON sink and the idempotence source of the JSON
  mode at once: when built it reads `OUTPUT_FILE` (if it exists) -- the same
  list of `RMUERegulation` that `JsonFileLoader` writes -- and indexes its
  documents by `dre_url`; as each city finishes, `record` adds its documents
  and rewrites the file. A document already present there is reused instead of
  reopening a browser / re-downloading its PDF for it, the same way a failed
  resolution/extraction isn't retried either. Without `json` in `load_targets`
  no `OutputStore` is built (`None` is passed instead) and this source doesn't
  exist.
- The database, only when `database` is in `load_targets`. At the start of
  the run, `find_processed_documents_task` (`tasks/FindProcessedDocuments.py`)
  reads every `RMUE` and `FeeRegulation` row back as a `RegulationDocument`
  (`pdf_url`, `status`, `raw_text` and `structure` included, the JSON column
  validated as `list[StructureNode]`), keyed by `dre_url`, and passes that
  `processed_by_url` dict to every `process_city_task`. This is what saves
  the work when `OUTPUT_FILE` is gone (another machine, a fresh container, a
  deleted file) or `json` isn't a target: every document already in Postgres
  is reused instead of reopening a browser for it. Without `database` in
  `load_targets` the task returns `{}` and the database is never opened.
- A `database`-only run therefore skips documents already in Postgres, and a
  `json`-only run skips documents already in `OUTPUT_FILE`; with both, either
  is enough.
- Either way, the document found is reused as-is, whatever its `status` -- a
  failed one isn't attempted again. Only the documents found in neither
  source are resolved and extracted.
- The set of documents to process still comes from the freshly parsed listing
  page, and `process_city_task` still returns each city's complete document
  set (reused documents included), so the `json` target and `OutputStore`
  always see everything. Only the persist step is narrower: `process_city_task`
  hands `persist_regulation_results_task` just the documents that are **not** in
  `processed_by_url` -- also when this run reused one from `OutputStore` --
  so each document is written to Postgres exactly once, on its first
  complete run. A municipality with no new document isn't persisted at all.
- As a consequence, a row already persisted isn't refreshed by later runs.
  To retry a document that failed (or to re-extract it), remove its entry
  from `OUTPUT_FILE` *and* delete its `RMUE`/`FeeRegulation` row (or, to
  retry everything, delete `OUTPUT_FILE` and those tables' rows) and run
  again.
