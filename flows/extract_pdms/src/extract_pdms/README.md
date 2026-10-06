# extract_pdms

Resolves every Portuguese municipality's PDM (Plano Diretor Municipal) via
the SNIT portal and extracts the text of every regulation PDF in its
history.

```
extract_pdms/
  ExtractPDM.py                 the flow: fans out one task run per municipality
  Settings.py / .env            concurrency limits, retries, and load_targets
  data/GetRegionsAndMunicipalitiesAsync.json  municipality list
  schema/                       PDMRecord, RegulationDocument, ExtractionState
  services/
    SnitSearch.py                searches SNIT for a municipality's PDM
    OutputStore.py                atomic, resumable per-document JSON cache
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
  out/state.json                OutputStore's resumable checkpoint (separate from pdms.json)
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
| `OUTPUT_FILE`                    | `extract_pdms/out/pdms.json`                                 | used when `json` is in `LOAD_TARGETS`         |
| `STATE_FILE`                     | `extract_pdms/out/state.json`                                | `OutputStore`'s resumable checkpoint           |
| `DATABASE_URL`                   | `postgres://minha_regiao:minha_regiao@localhost:5432/minha_regiao` | used when `database` is in `LOAD_TARGETS` (set `POSTGRES_PORT` at the repo root if 5432 is taken) |
| `LOAD_TARGETS`                   | `["json"]`                                                   | which sinks to write to — `database` (per municipality, as each finishes), `json` (once, at the end), or both (see [flows/CLAUDE.md](../../../CLAUDE.md) for the `Loader` pattern) |

## Running

```bash
uv run --package extract_pdms python -m extract_pdms.ExtractPDM

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run extract-pdms
```

## Persistence

The database is written **incrementally, by municipality**, not once at the end
of the run. `process_municipio_task` (`tasks/ProcessMunicipio.py`) has the
persist as its last step: after `process_municipio(...)` has resolved the
municipality's PDM(s) and every one of their documents has reached its final
state (and the result has been recorded in `OutputStore`), it calls
`persist_pdms_task(records, processed_by_url)` (`tasks/PersistPdms.py`) with
just that municipality's records. So a run that fails or is interrupted midway,
after hours of downloads and extraction, keeps everything the municipalities
already finished — nothing is lost for want of a final write.

- It only happens when `database` is in `LOAD_TARGETS`. A document is never
  persisted part-way through its processing.
- The task isn't called when there's nothing to write: the municipality has no
  records, or every document of every record is already in `processed_by_url`.
  (A record with no documents at all still counts as something to write: its
  `PDM` row is.)
- Several municipalities persist at the same time, from different threads;
  `minha_regiao.database.DatabaseManager.connection` serialises their Tortoise
  connections.
- If a persist fails, that municipality's task run fails (and the flow with it),
  but its results are already in `STATE_FILE`, and since its documents aren't in
  the database they're written on the next run without being processed again.
- The flow itself only waits for the task runs, logs a summary, and — with
  `json` in `LOAD_TARGETS` — writes `OUTPUT_FILE` once at the end (a single file
  with every municipality, so it can't be incremental).

## Idempotence

Idempotent and resumable at the document level (keyed by the document's
`url`, whatever its `status`): downloading and parsing a regulation PDF is the
expensive, failure-prone step, so it's skipped for any document that already
has a result. SNIT search and fetch are cheap metadata lookups and always
re-run. There are two sources for "already has a result", checked in this
order — see the docstring on `extract_pdms()` in `ExtractPDM.py`:

1. `OutputStore` (`services/OutputStore.py`) records each document's result in
   `STATE_FILE` as soon as it's produced, whatever `load_targets` is.
2. The database, only when `database` is in `LOAD_TARGETS`. At the start of
   the run, `find_processed_documents_task` reads every `PDMDocument` row
   back as a `RegulationDocument` (`status`, `text` and `structure`
   included, the JSON column validated as `list[StructureNode]`), keyed by
   `url`, and passes that `processed_by_url` dict to every `process_municipio_task`.
   This is what saves the work when the state file is gone (another machine,
   a fresh container, a deleted file): every document already in Postgres is
   reused instead of re-downloaded. Without `database` in `LOAD_TARGETS` the
   task returns `{}` and the database is never opened.

A document found in either source is reused as-is, even if its `status` is a
failure: it isn't attempted again. To retry one, remove its entry from
`STATE_FILE` *and* delete its `PDMDocument` row (or, to retry everything, the
whole `STATE_FILE` and the `pdm_document` rows).

When a municipality is persisted (see "Persistence"), a document that's in
`processed_by_url` is **not** written again, even when this run reused it from
`OutputStore`: each document reaches Postgres exactly once, on its first
complete run. `persist_pdms` still upserts each city's `PDM` row (`title`/`identifier`/`source_url`/`pdf_url`),
computed over *all* of the record's documents, but only writes a `PDMDocument`
row for the ones not already in `processed_by_url`. As a consequence, a row
already persisted isn't refreshed by later runs — fixing one means deleting
its row so it's processed again.
