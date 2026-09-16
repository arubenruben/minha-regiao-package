# Small, standalone image that runs this repo's Alembic migrations against
# DATABASE_URL. env.py (in this same folder) only touches alembic/
# sqlalchemy/asyncpg (see its own comment -- these are hand-written
# migrations against raw SQL, not generated from the Tortoise models),
# which are already dependencies of the `minha_regiao` workspace member
# alone, so this syncs only that package rather than the whole uv
# workspace -- in particular it never needs
# extract_pdms/extract_rmues/extract_election_files's scrapling[fetchers]
# extra (Playwright/Patchright browser install), which is most of what
# makes the main (repo-root) Dockerfile's image large.
#
# Built from the repo root as context (needs uv.lock and every workspace
# member's pyproject.toml to resolve `--package minha_regiao`), not from
# this directory: `docker build -f migrations/Dockerfile .`
FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /workspace
COPY . .

RUN uv sync --frozen --no-dev --package minha_regiao

ENV PATH="/workspace/.venv/bin:${PATH}"

# alembic.ini lives alongside this Dockerfile, not at the repo root --
# switch there so plain `alembic` (no `-c`) finds it.
WORKDIR /workspace/migrations

# `docker run <image>` runs `alembic upgrade head`; override the command to
# run any other alembic subcommand (e.g. `downgrade -1`, `current`).
ENTRYPOINT ["alembic"]
CMD ["upgrade", "head"]
