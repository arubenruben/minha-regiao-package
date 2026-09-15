# extract_election_files

Discovers Portugal's election result files (presidential, parliament,
European, regional, town hall) linked from the Ministério da
Administração Interna's elections site, extracts structured metadata for
each via an LLM, uploads the raw files, and publishes the combined
dataset.

```
extract_election_files/
  ElectionFiles.py           parent flow: discover sub-site links, run
                              every election-type sub-flow, structure and
                              publish the results
  Settings.py / .env         MAI base URL, LLM provider, HF, load_targets
  prompt/                    LLM prompt for structured metadata extraction
  schema/                    Election, MAIWebpage, StructuredElection, ...
  services/
    ElectionMetadataExtractor.py  runs the LLM extraction
    ElectionFileMatcher.py         matches/dedupes discovered files
    ElectionRawFileUploader.py     uploads each raw file to the HF dataset repo
    ElectionDatasetPublisher.py    publishes the structured dataset
  sub_flows/
    european_elections/            one sub-flow per election type; each
    parliament_elections/          discovers that type's files from its
    presidential_elections/        own section of the MAI site and
    regional_elections/            returns a list[Election]
    town_hall_elections_files/
```

## Prerequisites

- An OpenAI-compatible LLM provider (via OpenRouter by default) for
  structured metadata extraction — `OPENROUTER_API_KEY` and
  `OPENROUTER_MODEL`, or `GOOGLE_API_KEY`/`GEMINI_MODEL` if using Gemini
  directly.
- A Hugging Face token with write access to the destination dataset repo
  (`HF_API_KEY`) — required whenever `huggingface` is in `load_targets`
  (the default), since raw files are also uploaded there.
- Install the extras this flow needs: `uv sync --package extract_election_files`
  (or `--extra dev` for the whole project).

## Configuration

Settings are pydantic-settings, loaded from
`extract_election_files/.env` — copy `.env.example` to `.env` and fill in
values.

| Variable                | Default                        | Notes                                             |
|---------------------------|----------------------------------|------------------------------------------------------|
| `SEG_MAI_BASE_URL`         | _(required)_                     | MAI elections site base URL                          |
| `OPENROUTER_API_KEY`       | _(required)_                     |                                                        |
| `OPENROUTER_MODEL`         | _(required)_                     |                                                        |
| `OPENROUTER_BASE_URL`      | `https://openrouter.ai/api/v1`   |                                                        |
| `GOOGLE_API_KEY`           | _(none)_                         | alternative to OpenRouter                             |
| `GEMINI_MODEL`             | _(none)_                         |                                                        |
| `HF_API_KEY`               | _(required)_                     |                                                        |
| `HF_DATASET_REPO_ID`       | _(required)_                     | destination repo for raw files + structured metadata  |
| `LOAD_TARGETS`             | `["json"]`                       | `huggingface`, `json`, or both — see [flows/CLAUDE.md](../../../CLAUDE.md) |
| `OUTPUT_FILE`              | `extract_election_files/out/elections.json` | used when `json` is in `LOAD_TARGETS`   |

## Running

```bash
uv run --package extract_election_files python -m extract_election_files.ElectionFiles

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run extract-election-files
```

This flow only discovers and structures raw election files — parsing the
actual result numbers out of them is `parse_election_files`'s job.
