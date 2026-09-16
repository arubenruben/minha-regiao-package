# extract_pdms

Resolves every Portuguese municipality's PDM (Plano Diretor Municipal) via
the SNIT portal and extracts the text of every regulation PDF in its
history.

```
extract_pdms/
  ExtractPDM.py                 the flow: fans out one task run per municipality
  Settings.py / .env            concurrency limits, retries, and load_targets
  data/GetRegionsAndMunicipalitiesAsync.json  municipality list
  schema/                       PDMRecord, RegulationDocument, ExtractionState
  services/
    SnitSearch.py                searches SNIT for a municipality's PDM
    StructureParser.py            parses a notice's text into its legal structure
    OutputStore.py                atomic, resumable per-document JSON cache
    PDMRepository.py              persists PDMRecord -> the PDM table
    ConcurrencyLimiter.py         registers the flow's tag-based concurrency limit
  exception/                    typed exceptions for PDF url/page-action failures
  out/pdms.json                 default JSON output (see load_targets below)
```

Downloading/parsing a regulation PDF and narrowing it down to its own notice
(a raw DR page range bundles unrelated notices from other
municipalities/entities) is shared, not extract_pdms-specific -- see
`minha_regiao.gazette` (`PdfTextExtractor.py`, `GazetteSegmenter.py`, and
their exceptions), also used by `extract_rmues`.

## Prerequisites

- A running Postgres instance (see `dev.docker-compose.yml` at the repo
  root — `docker compose -f dev.docker-compose.yml up -d`) if `database` is
  in `load_targets`.
- Install the extras this flow needs: `uv sync --package extract_pdms`
  (or `--extra dev` for the whole project).

## Configuration

Settings are pydantic-settings, loaded from `extract_pdms/.env` — copy
`.env.example` to `.env` and fill in values.

| Variable                       | Default                                                    | Notes                                        |
|---------------------------------|-------------------------------------------------------------|-----------------------------------------------|
| `SNIT_CONCURRENCY`               | `4`                                                          | municipalities processed in parallel          |
| `SNIT_TASK_RETRIES`              | `3`                                                          | SNIT portal retries on a transient bad response |
| `SNIT_RETRY_DELAY_SECONDS`       | `5.0`                                                        |                                                |
| `PDF_DOWNLOAD_CONCURRENCY`       | `8`                                                          | regulation PDFs downloaded/parsed in parallel |
| `PDF_DOWNLOAD_TIMEOUT_SECONDS`   | `60.0`                                                       |                                                |
| `OUTPUT_FILE`                    | `extract_pdms/out/pdms.json`                                 | used when `json` is in `LOAD_TARGETS`         |
| `DATABASE_URL`                   | `postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao` | used when `database` is in `LOAD_TARGETS`     |
| `LOAD_TARGETS`                   | `["json"]`                                                   | which sinks to write to — `database`, `json`, or both (see [flows/CLAUDE.md](../../../CLAUDE.md) for the `Loader` pattern) |

## Running

```bash
uv run --package extract_pdms python -m extract_pdms.ExtractPDM

# or, via the CLI (see cli/README.md):
uv run --package minha_regiao_cli minha-regiao run extract-pdms
```

Idempotent and resumable at the document level: `OutputStore` records each
regulation document's download/text-extraction result as soon as it's
produced, and a document already recorded is reused on the next run
instead of being re-downloaded and re-parsed — see the docstring on
`extract_pdms()` in `ExtractPDM.py`.
