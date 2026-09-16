# extract_rmues

Extracts and indexes RMUE (Regulamento Municipal de Urbanização e
Edificação) and municipal fee regulations, published on the Diário da
República website, by city.

```
extract_rmues/
  ExtractRMUEs.py          the flow: parse the RMUE page, persist, then
                            resolve each document's PDF url
  Settings.py / .env       source URL, database, load_targets, concurrency
  schema/                  RMUERegulation, PendingDocument
  services/
    RMUEPageParser.py       parses the DR listing page
    RegulationMetadata.py   extracts a document's year/completeness from its name
    PDFResolver.py          resolves a DR detail page to its PDF url
    RMURepository.py        persists RMUERegulation -> RMUE/FeeRegulation tables
    ConcurrencyLimiter.py   registers Prefect tag-based concurrency limits
  tasks/                   one @task per flow step; resolve_pdf_url is
                            tagged and fanned out via .map(), capped by
                            pdf_resolve_concurrency (see flows/CLAUDE.md)
```

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
| `DATABASE_URL`     | `postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao`   | used when `database` is in `LOAD_TARGETS`         |
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
page — this step isn't gated by `load_targets`, since it's a required
enrichment of already-persisted rows rather than a load step.
