# Project logic for agents

This project produces reproducible ETL pipelines that extract geographic and
electoral information about Portugal, and it is open source with a
scientific-reproducibility scope: a stranger should be able to clone this
repo and independently reproduce the datasets it publishes.

Every decision about distribution, packaging, or where a feature belongs
should be checked against the three audiences below. Most confusion in this
repo comes from conflating them — keep them separate.

## The three audiences

1. **"I just want the data."** — Zero setup, no Docker, no Python. Point them
   at the published HuggingFace datasets (e.g. `minharegiao/portuguese-geo`,
   `minharegiao/portuguese-elections`). This is the primary open-data
   distribution channel and should be the first thing the root README says.
   This audience never needs the API or the database.

2. **"I want to reproduce or extend a pipeline."** — The scientific/OSS
   reproducibility case. Needs `docker compose up` for Postgres and a
   Prefect server, and a way to run one flow without memorizing
   `uv run --package extract_geo python -m extract_geo.ExtractGeo`. This is
   the audience that justifies a CLI entrypoint and containerizing the flows
   — work in `flows/` and any CLI/Docker tooling should be built for this
   audience first.

3. **"I want to run the API against my own data."** — Same Docker Compose,
   plus an `api` service, entirely self-hosted and separable from any
   maintainer-hosted instance.

## Implications that follow from this

- **The API is not the OSS deliverable.** The pipelines and the datasets
  they publish are. The API (`api/`) is a convenience layer that happens to
  be open source too, but it is fine for a maintainer-hosted instance of it
  to be gated, rate-limited, or private — hosting infrastructure costs
  money, and the underlying data is already open via HuggingFace regardless
  of whether anyone can query the hosted API. Never assume "make the API
  public" is required to fulfill the open-source goal.
- **Don't build audience-3 features into audience-2 tooling, or vice versa.**
  e.g. the CLI/Docker work for running a single flow (audience 2) should not
  assume or require the API service is running, and API-specific config
  (auth, rate limiting) should not leak into flow `Settings.py` files.
- **Settings, not hardcoding, for anything environment-shaped** — this
  already applies to concurrency limits (see [flows/CLAUDE.md](flows/CLAUDE.md))
  and extends to load targets, dataset repo IDs, and API auth: these are
  `pydantic-settings` fields with `.env.example` documentation, never
  literals.
- **Prefer extending the existing shared package (`minha_regiao`) over
  duplicating logic per flow or in `api/`.** Entities, the database manager,
  and any new cross-cutting abstraction (e.g. a `Loader` interface for
  choosing database/JSON/HuggingFace output sinks) belong there, since both
  `flows/*` and `api` already depend on it via the uv workspace.

## Where to look next

- [flows/CLAUDE.md](flows/CLAUDE.md) — mandatory tag-based concurrency
  pattern for any flow that fans out per-item task runs.
- Root `README.md` — the entry point that should route each of the three
  audiences above to the right instructions.
