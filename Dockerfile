# Single image for the whole uv workspace: every flow, the CLI, and (once
# built out) the API all come from one `uv sync --frozen` against the one
# shared uv.lock, so this image can run any of them depending on the CMD it's
# given (see docker-compose.yml's `cli` service).
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

WORKDIR /workspace
COPY . .

RUN uv sync --frozen --no-dev

# extract_pdms, extract_rmues, and extract_election_files all depend on
# scrapling[fetchers] (Playwright/Patchright-backed browsers) -- without
# this, any flow that crawls a site fails immediately inside the container.
# `scrapling install` pulls the browser binaries and their OS-level
# dependencies (equivalent to `playwright install --with-deps chromium`).
RUN uv run scrapling install

ENV PATH="/workspace/.venv/bin:${PATH}"

ENTRYPOINT []
CMD ["minha-regiao", "--help"]
