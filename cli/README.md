# minha-regiao-cli

A thin CLI over every flow package in this workspace, so reproducing a
pipeline doesn't require memorizing
`uv run --package extract_geo python -m extract_geo.ExtractGeo` by heart.
Each flow still runs exactly as it would if invoked directly — this just
imports and calls it in-process.

## Installing

From the repo root:

```bash
uv sync --package minha_regiao_cli
```

## Usage

```bash
# List the flow names accepted by `run`
uv run --package minha_regiao_cli minha-regiao list

# Run one
uv run --package minha_regiao_cli minha-regiao run extract-geo
```

Each flow reads its own configuration the same way it does when run
directly (its package's `.env` / `Settings.py`) — see that flow's own
README for prerequisites and configuration.
