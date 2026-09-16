# extract_rmues

Extracts and indexes RMUE (Regulamento Municipal de Urbanização e
Edificação) and municipal fee regulations, published on the Diário da
República website, by city.

```
extract_rmues/
  ExtractRMUEs.py          the flow: parse the RMUE page, persist, then
                            resolve each document's PDF url
  Settings.py / .env       source URL, database, load_targets, concurrency
  schema/                  RMUERegulation, PendingDocument, ResolutionState
  services/
    RMUEPageParser.py       parses the DR listing page
    RegulationMetadata.py   extracts a document's year/completeness/notice
                            metadata (doc_type/number/year) from its name
    PDFResolver.py          resolves a DR detail page to its PDF url
    RMURepository.py        persists RMUERegulation -> RMUE/FeeRegulation tables
    ResolutionStore.py      caches resolved PDF urls across runs when no
                            database is configured, keyed by dre_url --
                            see "Idempotence" below
    ConcurrencyLimiter.py   registers Prefect tag-based concurrency limits
  tasks/                   one @task per flow step; resolve_pdf_url and
                            extract_notice_text are tagged and fanned out via
                            .map(), capped by pdf_resolve_concurrency /
                            pdf_extract_concurrency (see flows/CLAUDE.md)
```

Downloading a resolved PDF and narrowing it down to its own notice text uses
`minha_regiao.gazette` (shared with `extract_pdms`, since these are the same
kind of raw DR gazette page range) — see `tasks/ExtractNoticeText.py`. Not
yet persisted anywhere: `RMUE`/`FeeRegulation` have no column for it yet, so
this currently only proves the extraction works end to end.

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
| `PDF_RESOLVE_CONCURRENCY` | `8`                                                            | DR detail pages resolved to a PDF url in parallel (each opens its own browser) |
| `PDF_EXTRACT_CONCURRENCY` | `8`                                                            | resolved PDFs downloaded and segmented to their own notice text in parallel |
| `PDF_EXTRACT_TIMEOUT_SECONDS` | `60.0`                                                     | timeout for downloading a single PDF                                          |
| `DATABASE_URL`     | `postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao`   | used when `database` is in `LOAD_TARGETS`         |
| `RESOLUTION_STATE_FILE` | `extract_rmues/out/rmue_resolved_documents.json`                 | used when `database` is *not* in `LOAD_TARGETS` — see "Idempotence" below |
| `LOAD_TARGETS`     | `["json"]`                                                            | `database`, `json`, or both — see [flows/CLAUDE.md](../../../CLAUDE.md) |
| `OUTPUT_FILE`      | `extract_rmues/out/rmues.json`                                       | used when `json` is in `LOAD_TARGETS`             |

## Running

```bash
uv run --package extract_rmues python -m extract_rmues.ExtractRMUEs

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run extract-rmues
```

After persisting the parsed regulations, the flow always runs a second
pass resolving each document's PDF url from its Diário da República detail
page — this step itself isn't gated by `load_targets`, since it's a required
enrichment rather than a load step. Its document source is gated, though:
with `database` in `load_targets` it resolves every document still missing a
`pdf_url` in Postgres (including backlog from prior runs) and writes
resolved urls back there; with the default `["json"]`, it resolves only this
run's freshly-parsed documents in memory — so the flow needs no database at
all.

## Idempotence

Resolving a PDF url opens its own headless browser session per document, so
re-resolving one that's already been resolved is expensive and worth
skipping on a re-run:

- With `database` in `load_targets`, this falls out of the existing query —
  `find_documents_missing_pdf_url` only ever returns rows where `pdf_url` is
  still null, so an already-resolved row (in this run or any prior one)
  is never re-queued.
- Without a database, there's no persisted row to check, so
  `ResolutionStore` (`services/ResolutionStore.py`) plays that role instead:
  it persists every resolved document — keyed by `dre_url`, and regardless
  of whether a PDF url was actually found — to `RESOLUTION_STATE_FILE`
  between runs. A document already present there is reused instead of
  reopening a browser for it, the same way a failed resolution isn't
  retried either. Only relevant when `database` isn't in `load_targets`;
  Postgres is already the source of truth otherwise.
