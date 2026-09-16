# minha-regiao

Open-source ETL pipelines that extract geographic and electoral
information about Portugal — municipalities, districts, parishes, PDMs
(municipal master plans), RMUEs (municipal building regulations), and
election results — built as reproducible Prefect flows.

This repo serves three different audiences. See [AGENTS.md](AGENTS.md) for
the full reasoning behind that split; the short version is below.

## 1. I just want the data

No setup required. The pipelines publish their output to public Hugging
Face datasets:

- [`minharegiao/portuguese-geo`](https://huggingface.co/datasets/minharegiao/portuguese-geo) — cities, districts, parishes
- [`minharegiao/portuguese-elections`](https://huggingface.co/datasets/minharegiao/portuguese-elections) — election results

Load either with the `datasets` library, or download individual files
directly from the Hugging Face UI.

## 2. I want to reproduce or extend a pipeline

Everything runs in containers — no local Python/browser setup needed.
This assumes you already have a Prefect instance running somewhere
(Prefect Cloud, or your own `prefect server start`); `docker compose`
doesn't start one for you.

```bash
# Bring up Postgres
docker compose up -d

# Point at your running Prefect instance, then see the available flows
export PREFECT_API_URL=http://localhost:4200/api
docker compose run --rm cli minha-regiao list

# Run one
docker compose run --rm cli minha-regiao run extract-geo
```

A flow that needs credentials (Hugging Face, an LLM provider, ...) reads
them from its own package's environment — pass them per-run, e.g.:

```bash
docker compose run --rm -e HF_API_KEY=... cli minha-regiao run extract-geo
```

### Without Docker

For local development on a single flow (faster iteration, debugger
support), install [uv](https://docs.astral.sh/uv/) and run flows directly
against a local Postgres:

```bash
docker compose -f dev.docker-compose.yml up -d   # Postgres only
uv sync
uv run --package minha_regiao_cli minha-regiao run extract-geo
# or, equivalently:
uv run --package extract_geo python -m extract_geo.ExtractGeo
```

Each flow package under [flows/](flows/) has its own README with
prerequisites and configuration. Flow-level conventions (concurrency
limits, the composable `Loader` pattern for choosing where output goes)
are documented in [flows/CLAUDE.md](flows/CLAUDE.md).

## 3. I want to run the API against my own data

Coming soon — `api/` isn't built out yet. Once it is, it'll be part of
the same `docker compose up`, entirely self-hosted and separate from any
maintainer-hosted instance of it.

## Repository layout

```
minha_regiao/    shared package: entity models, database access, LLM
                  strategies, and the Loader abstraction
flows/            one independent Prefect-flow package per pipeline
cli/              `minha-regiao` command, wraps every flow package
api/              (in progress) read API over the database
migrations/       Alembic migrations for the shared Postgres schema
```

This is a `uv` workspace — every package resolves against one shared
`uv.lock`, so `uv sync` from the repo root installs everything at once.
