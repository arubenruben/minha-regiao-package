# extract_rmues

Extracts and indexes RMUE (Regulamento Municipal de Urbanização e
Edificação) and municipal fee regulations, published on the Diário da
República website, by city.

```
extract_rmues/
  ExtractRMUEs.py          the flow: parse the RMUE page, persist, then
                            fan out one task run per municipality to
                            resolve/extract its documents
  Settings.py / .env       source URL, database, load_targets, concurrency
  schema/                  RMUERegulation, PendingDocument, RMUEExtractionState
  services/
    RMUEPageParser.py       parses the DR listing page
    RegulationMetadata.py   extracts a document's year/completeness/notice
                            metadata (doc_type/number/year) from its name
    PDFResolver.py          resolves a DR detail page to its PDF url
    RMURepository.py        persists RMUERegulation -> RMUE/FeeRegulation tables
                            (rows, then each document's pdf_url/status/
                            raw_text/structure)
    OutputStore.py          atomic, resumable per-document JSON cache
    ConcurrencyLimiter.py   registers Prefect tag-based concurrency limits
  tasks/                   one @task per flow step; process_city is fanned
                            out via .map() (one task run per municipality,
                            capped by city_concurrency); within it,
                            resolve_pdf_url and extract_notice_text are
                            further tagged and fanned out over that city's
                            own documents, capped by pdf_resolve_concurrency
                            / pdf_extract_concurrency (see flows/CLAUDE.md)
  out/rmues.json           default JSON output (see load_targets below)
  out/rmue_state.json      OutputStore's resumable checkpoint (separate from rmues.json)
```

Downloading a resolved PDF, narrowing it down to its own notice text, and
parsing that notice's legal Parte/Título/Capítulo/Secção/Subsecção/Artigo
structure uses `minha_regiao.gazette` (shared with `extract_pdms`, since
these are the same kind of raw DR gazette page range) — see
`tasks/ExtractNoticeText.py`. The result -- `pdf_url`, `status`, `raw_text`,
and `structure` -- is recorded onto each document in `RMUERegulation` (see
`schema/RMUERegulation.py`), and, when `database` is in `LOAD_TARGETS`,
written back onto the matching `RMUE`/`FeeRegulation` row by
`tasks/PersistRegulationResults.py` -- not just the JSON output.

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
| `LOAD_TARGETS`     | `["json"]`                                                            | `database`, `json`, or both — see [flows/CLAUDE.md](../../../CLAUDE.md) |
| `OUTPUT_FILE`      | `extract_rmues/out/rmues.json`                                       | used when `json` is in `LOAD_TARGETS`             |
| `STATE_FILE`       | `extract_rmues/out/rmue_state.json`                                  | `OutputStore`'s resumable checkpoint — see "Idempotence" below |

## Running

```bash
uv run --package extract_rmues python -m extract_rmues.ExtractRMUEs

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run extract-rmues
```

After persisting the parsed regulations, the flow fans out one task run per
municipality (`process_city_task`, see `tasks/ProcessCity.py`) to resolve
that city's documents' PDF urls and extract their own notice text/
structure. Its document source is gated by `load_targets`: with `database`
it resolves every document still missing a `pdf_url` in Postgres (including
backlog from prior runs); with the default `["json"]`, it resolves only
this run's freshly-parsed documents in memory — so the flow needs no
database at all. Once every municipality is processed, with `database` in
`load_targets` each document's `pdf_url`/`status`/`raw_text`/`structure` is
written back onto its `RMUE`/`FeeRegulation` row (`tasks/
PersistRegulationResults.py`).

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

- `OutputStore` (`services/OutputStore.py`) persists every document's
  result — keyed by `dre_url`, regardless of whether it succeeded — to
  `STATE_FILE` as soon as each city's processing finishes. A document
  already present there is reused instead of reopening a browser /
  re-downloading its PDF for it, the same way a failed
  resolution/extraction isn't retried either.
- With `database` in `load_targets`, a document whose `pdf_url` has already
  been persisted to Postgres also falls out of `find_documents_missing_pdf_url`
  entirely on a later run — but its enriched record (text/structure) stays
  in `STATE_FILE`/`OUTPUT_FILE` from the run that processed it:
  `process_city_task` merges each run's (often partial) results onto
  whatever `OutputStore` already has for that city, rather than replacing
  it outright.
