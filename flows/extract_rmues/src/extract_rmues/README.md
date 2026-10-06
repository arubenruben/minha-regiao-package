# extract_rmues

Extracts and indexes RMUE (Regulamento Municipal de Urbanização e
Edificação) and municipal fee regulations, published on the Diário da
República website, by city.

```
extract_rmues/
  ExtractRMUEs.py          the flow: parse the RMUE page, fan out one task
                            run per municipality to resolve/extract its
                            documents, then write the results
  Settings.py / .env       source URL, database, load_targets, concurrency
  schema/                  RMUERegulation, RMUEExtractionState
  services/
    RMUEPageParser.py       parses the DR listing page
    RegulationMetadata.py   extracts a document's year/completeness/notice
                            metadata (doc_type/number/year) from its name
    PDFResolver.py          resolves a DR detail page to its PDF url
    RMURepository.py        persists RMUERegulation -> RMUE/FeeRegulation tables
                            (one upserted row per document, with its final
                            pdf_url/status/raw_text/structure)
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
written to the matching `RMUE`/`FeeRegulation` row by
`tasks/PersistRegulationResults.py` -- not just the JSON output.

An amending aviso quotes each article it changes and then republishes the
consolidated regulation, so the same Artigo can come out of the parser twice.
`parse_structure` collapses those copies (see
`minha_regiao.gazette.StructureDeduplicator`), and `services/RMURepository.py`
applies the same deduplication again before persisting `structure`, logging a
warning for what it removes -- so a structure read back from an earlier run's
state file, parsed before this existed, is cleaned too. Since every run
upserts every document, rows already persisted with duplicates are
overwritten with the cleaned structure on the next run.

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

The flow has a single path, independent of `load_targets`: the documents it
processes always come from the listing page parsed in the current run
(`parse_rmue_page_task`), never from the database. It maps
`process_city_task` (see `tasks/ProcessCity.py`) directly over the parsed
entries, one task run per municipality. Each run resolves the PDF url of
that city's `urbanization_documents` and `fee_documents` (separately) and
extracts their own notice text/structure, and returns the city's *complete*
set of documents -- a document whose PDF url couldn't be resolved comes
back with `status=pdf_url_not_found`.

Nothing is written to the database while the cities are being processed.
Once every municipality is done, one call writes all the resolved entries to
each sink in `load_targets`: with `database`, `tasks/PersistRegulationResults.py`
upserts one `RMUE`/`FeeRegulation` row per document, keyed by
`(city, dre_url)`, with its `year`/`name`/`is_complete` and its final
`pdf_url`/`status`/`raw_text`/`structure`; with `json`, the same entries go to
`OUTPUT_FILE`. So a document reaches Postgres exactly once, already
complete -- an interrupted run leaves no half-filled rows behind -- and the
flow needs no database at all with the default `["json"]`. Municipalities
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

- `OutputStore` (`services/OutputStore.py`) persists every document's
  result — keyed by `dre_url`, regardless of whether it succeeded — to
  `STATE_FILE` as soon as each city's processing finishes. It is always
  written, whatever `load_targets` is. A document already present there is
  reused instead of reopening a browser / re-downloading its PDF for it,
  the same way a failed resolution/extraction isn't retried either.
- Because every run starts from the freshly parsed listing page and
  `process_city_task` returns each city's complete document set (cached
  documents included), there is nothing to merge with what `OutputStore`
  already has for a city, and the database never decides what gets
  processed. With `database` in `load_targets`, an unchanged document is
  simply upserted again, by `(city, dre_url)`, with the same values it
  already has.
- To retry a document that failed, remove its entry (or the whole
  `STATE_FILE`) and run again.
