# extract_pdms

Resolves every Portuguese municipality's PDM (Plano Diretor Municipal) via
the SNIT portal and extracts the text of every regulation PDF in its
history.

```
extract_pdms/
  ExtractPDM.py                 the flow: fans out one task run per municipality
  Settings.py / .env            concurrency limits, retries, and load_targets
  data/GetRegionsAndMunicipalitiesAsync.json  municipality list
  schema/                       PDMRecord, RegulationDocument
  services/
    SnitSearch.py                searches SNIT for a municipality's PDM
    OutputStore.py                the JSON sink (rewrites out/pdms.json after
                                    every municipality) and, reading that same
                                    file back, the JSON mode's per-document
                                    idempotence
    PDMRepository.py              persists PDMRecord -> the PDM table (one
                                    row per city, title/identifier/latest
                                    pdf_url) plus one PDMDocument row per
                                    regulation document (full history, each
                                    with its own status/text/structure);
                                    also reads every PDMDocument back as a
                                    RegulationDocument, keyed by url
                                    (find_processed_documents)
    ConcurrencyLimiter.py         registers the flow's tag-based concurrency limit
  tasks/                        one @task per flow step; process_municipio is
                                  mapped per municipality and, as its last
                                  step, calls persist_pdms
                                  (tasks/PersistPdms.py) for that municipality
  exception/                    typed exceptions for PDF url/page-action failures
  out/pdms.json                 default JSON output (see load_targets below)
```

Downloading/parsing a regulation PDF, narrowing it down to its own notice (a
raw DR page range bundles unrelated notices from other
municipalities/entities), and parsing that notice's legal
Parte/Título/Capítulo/Secção/Subsecção/Artigo structure is shared, not
extract_pdms-specific -- see `minha_regiao.gazette` (`PdfTextExtractor.py`,
`GazetteSegmenter.py`, `StructureParser.py`, `RegulationStructure.py`,
`StructureDeduplicator.py`, and their exceptions), also used by
`extract_rmues`.

An amending aviso quotes each article it changes and then republishes the
consolidated regulation, so the same Artigo can come out of the parser twice.
`parse_structure` collapses those copies, and `services/PDMRepository.py`
applies the same deduplication again before persisting `structure`, logging a
warning for what it removes -- so a structure read back from an earlier run's
JSON output, parsed before this existed, is cleaned too. Rows already
persisted with duplicates are not reprocessed.

## Prerequisites

- A running Postgres instance (see `dev.docker-compose.yml` at the repo
  root — `docker compose -f dev.docker-compose.yml up -d`) if `database` is
  in `load_targets`.
- Install the extras this flow needs: `uv sync --package extract_pdms`
  (or `--extra dev` for the whole project).

## Configuration

Settings are pydantic-settings, loaded from `extract_pdms/.env` — copy
`.env.example` to `.env` and fill in values.

| Variable                       | Default                                                    | Notes                                        |
|---------------------------------|-------------------------------------------------------------|-----------------------------------------------|
| `SNIT_CONCURRENCY`               | `4`                                                          | municipalities processed in parallel          |
| `SNIT_TASK_RETRIES`              | `3`                                                          | SNIT portal retries on a transient bad response |
| `SNIT_RETRY_DELAY_SECONDS`       | `5.0`                                                        |                                                |
| `PDF_DOWNLOAD_CONCURRENCY`       | `8`                                                          | regulation PDFs downloaded/parsed in parallel |
| `PDF_DOWNLOAD_TIMEOUT_SECONDS`   | `60.0`                                                       |                                                |
| `OUTPUT_FILE`                    | `extract_pdms/out/pdms.json`                                 | used when `json` is in `LOAD_TARGETS`; rewritten after every municipality and read back at the start of the run as the JSON mode's idempotence (see "Idempotence") |
| `DATABASE_URL`                   | `postgres://minha_regiao:minha_regiao@localhost:5432/minha_regiao` | used when `database` is in `LOAD_TARGETS` (set `POSTGRES_PORT` at the repo root if 5432 is taken) |
| `LOAD_TARGETS`                   | `["json"]`                                                   | which sinks to write to, each per municipality as it finishes — `database`, `json`, or both (see [flows/CLAUDE.md](../../../CLAUDE.md) for the `Loader` pattern) |

There is no `STATE_FILE` any more: `OUTPUT_FILE` is both the output and the
checkpoint. A `state.json` left over from an earlier version is no longer read
and can be deleted.

## Running

```bash
uv run --package extract_pdms python -m extract_pdms.ExtractPDM

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run extract-pdms
```

## Persistence

Both sinks are written **incrementally, by municipality**, not once at the end
of the run. `process_municipio_task` (`tasks/ProcessMunicipio.py`) writes them
as its last steps: after `process_municipio(...)` has resolved the
municipality's PDM(s) and every one of their documents has reached its final
state, it calls `persist_pdms_task(records, processed_by_url)`
(`tasks/PersistPdms.py`) with just that municipality's records and then
`OutputStore.record(municipio, records)`. So a run that fails or is interrupted
midway, after hours of downloads and extraction, keeps everything the
municipalities already finished — nothing is lost for want of a final write.

- The database persist only happens when `database` is in `LOAD_TARGETS`. A
  document is never persisted part-way through its processing.
- `OutputStore` is only built when `json` is in `LOAD_TARGETS`; otherwise the
  tasks get `None` and skip it. `OutputStore.record` upserts the municipality's
  records (keyed by `(municipio, identifier)`) and rewrites the whole
  `OUTPUT_FILE` through `JsonFileLoader` (atomically), under a lock, so two
  municipalities finishing at the same time can't write it out of order. It's
  the last step, after the database persist: a municipality only counts as
  processed for the JSON mode once every other configured sink has it too.
- The task isn't called when there's nothing to write: the municipality has no
  records, or every document of every record is already in `processed_by_url`.
  (A record with no documents at all still counts as something to write: its
  `PDM` row is.)
- Several municipalities persist at the same time, from different threads;
  `minha_regiao.database.DatabaseManager.connection` serialises their Tortoise
  connections.
- If a persist fails, that municipality's task run fails (and the flow with it)
  before it reaches `OutputStore.record`, so it isn't in `OUTPUT_FILE` either and
  is processed again on the next run (unless its documents were already in the
  database).
- The flow itself only waits for the task runs and logs a summary.

## Idempotence

Idempotent and resumable at the document level (keyed by the document's
`url`, whatever its `status`): downloading and parsing a regulation PDF is the
expensive, failure-prone step, so it's skipped for any document that already
has a result. SNIT search and fetch are cheap metadata lookups and always
re-run. There are two sources for "already has a result", checked in this
order — see the docstring on `extract_pdms()` in `ExtractPDM.py`:

1. `OutputStore` (`services/OutputStore.py`), only when `json` is in
   `LOAD_TARGETS`. It is the JSON sink and the idempotence source of the JSON
   mode at once: when built it reads `OUTPUT_FILE` (if it exists) — the same
   list of `PDMRecord` that `JsonFileLoader` writes — and indexes its documents
   by `url`; as each municipality finishes, `record` adds its documents and
   rewrites the file. A document already in `OUTPUT_FILE` is reused instead of
   re-downloaded. Without `json` in `LOAD_TARGETS` no `OutputStore` is built
   (`None` is passed instead) and this source doesn't exist.
2. The database, only when `database` is in `LOAD_TARGETS`. At the start of
   the run, `find_processed_documents_task` reads every `PDMDocument` row
   back as a `RegulationDocument` (`status`, `text` and `structure`
   included, the JSON column validated as `list[StructureNode]`), keyed by
   `url`, and passes that `processed_by_url` dict to every `process_municipio_task`.
   This is what saves the work when `OUTPUT_FILE` is gone (another machine,
   a fresh container, a deleted file) or `json` isn't a target: every document
   already in Postgres is reused instead of re-downloaded. Without `database`
   in `LOAD_TARGETS` the task returns `{}` and the database is never opened.

A `database`-only run therefore skips documents already in Postgres, and a
`json`-only run skips documents already in `OUTPUT_FILE`; with both, either is
enough.

A document found in either source is reused as-is, even if its `status` is a
failure: it isn't attempted again. To retry one, remove it from `OUTPUT_FILE`
*and* delete its `PDMDocument` row (or, to retry everything, delete
`OUTPUT_FILE` and the `pdm_document` rows).

When a municipality is persisted (see "Persistence"), a document that's in
`processed_by_url` is **not** written again, even when this run reused it from
`OutputStore`: each document reaches Postgres exactly once, on its first
complete run. `persist_pdms` still upserts each city's `PDM` row (`title`/`identifier`/`source_url`/`pdf_url`),
computed over *all* of the record's documents, but only writes a `PDMDocument`
row for the ones not already in `processed_by_url`. As a consequence, a row
already persisted isn't refreshed by later runs — fixing one means deleting
its row so it's processed again.
