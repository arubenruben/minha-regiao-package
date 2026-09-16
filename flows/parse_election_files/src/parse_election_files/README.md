# parse_election_files

Downloads the raw election result files published by `extract_election_files`
and, eventually, parses each election type's actual result numbers into
the database.

**Status: work in progress.** `ParseElections.py`'s `parse_elections()`
flow downloads the raw files today; the five per-election-type sub-flows
under `sub_flows/` (european, parliament, presidential, regional,
town-hall) are stubs (`def town_hall_elections(paths): pass`, etc.) with
no parsing or persistence logic yet. Each stub's `Settings.py` already has
a `load_targets` field scaffolded for whoever implements it — see
[flows/CLAUDE.md](../../../CLAUDE.md) for the `Loader` pattern
(`extract_pdms` and `extract_rmues` are worked examples to follow).

```
parse_election_files/
  ParseElections.py         parent flow: loads the raw-files dataset and
                              downloads each file
  Settings.py / .env        Hugging Face dataset to read from
  services/
    ElectionFileDownloader.py  downloads one raw file by path
  sub_flows/
    european_elections/       one sub-flow per election type; each is
    parliament_elections/     currently a stub taking the downloaded
    presidential_elections/   file paths and returning nothing
    regional_elections/
    town_hall_elections/
```

## Prerequisites

- Read access to the Hugging Face dataset `extract_election_files`
  publishes to (`HF_API_KEY` if the repo is private).
- Install the extras this flow needs: `uv sync --package parse_election_files`
  (or `--extra dev` for the whole project).

## Configuration

Settings are pydantic-settings, loaded from
`parse_election_files/.env` — copy `.env.example` to `.env` and fill in
values.

| Variable                 | Default             | Notes                                  |
|----------------------------|----------------------|------------------------------------------|
| `HF_API_KEY`                | _(required)_         |                                            |
| `HF_DATASET_REPO_ID`        | _(required)_         | the repo `extract_election_files` publishes to |
| `HF_DATASET_CONFIG_NAME`    | `raw_election_files` |                                            |

## Running

```bash
uv run --package parse_election_files python -m parse_election_files.ParseElections

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run parse-election-files
```
