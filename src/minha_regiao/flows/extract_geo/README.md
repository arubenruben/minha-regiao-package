# extract_geo

Extracts Portuguese cities and districts, persists them to the database, and
publishes both datasets to the same Hugging Face dataset repo
(`minharegiao/portuguese-geo`, configs `cities` and `districts`).

```
extract_geo/
  ExtractGeo.py                       parent flow: runs cities, then districts
  Settings.py / .env                  shared settings (DB, Hugging Face)
  data/districts.json                 district reference data (name + INE prefix)
  schema/DistrictReference.py         shared schema for the reference data above
  services/DatasetPublisher.py        generic Hugging Face dataset publisher
  services/DatasetRepo.py             ensures a HF dataset repo exists
  services/DistrictReferenceLoader.py loads/matches district reference data
  sub_flows/
    extract_cities/                   ANMP contacts -> City table -> `cities` config
    extract_districts/                district reference -> District table,
                                       assigns each city its district -> `districts` config
```

`extract_cities` must run before `extract_districts`: district assignment
reads the `City` rows that `extract_cities` persists. `ExtractGeo.py` runs
them in that order for you; the sub-flows are only meant to be run
independently for local debugging of one half of the pipeline.

## Prerequisites

- A running Postgres instance (see `dev.docker-compose.yml` at the repo
  root — `docker compose -f dev.docker-compose.yml up -d`).
- A Hugging Face token with write access to `minharegiao/portuguese-geo`
  (only needed for the publish step; leave `HF_API_KEY` unset to skip auth
  and let the rest of the flow run against a local/test repo).
- Install the extras this flow needs: `pip install -e ".[extract_geo]"`
  (or `.[dev]` for the whole project).

## Configuration

Settings are layered: `extract_geo/Settings.py` holds what's shared by both
sub-flows, and each sub-flow has its own `Settings.py` for the fields only it
needs. Copy each `.env.example` to `.env` next to it and fill in values.

**`extract_geo/.env`** (shared)

| Variable            | Default                                                    | Notes                                   |
|---------------------|-------------------------------------------------------------|------------------------------------------|
| `DATABASE_URL`       | `postgres://minha_regiao:minha_regiao@localhost:9001/minha_regiao` | matches `dev.docker-compose.yml`         |
| `HF_API_KEY`         | _(none)_                                                   | Hugging Face write token                 |
| `GEO_DATASET_REPO_ID`| `minharegiao/portuguese-geo`                               | destination repo for both configs        |

**`extract_geo/sub_flows/extract_cities/.env`**

| Variable                                | Default                                              | Notes                                             |
|------------------------------------------|-------------------------------------------------------|-----------------------------------------------------|
| `ANMP_TOWN_HALL_URL`                     | ANMP town hall contacts page                          |                                                      |
| `ANMP_MUNICIPAL_ASSEMBLY_URL`            | ANMP municipal assembly contacts page                 |                                                      |
| `ELECTION_RESULTS_DATASET_REPO_ID`       | `minharegiao/portuguese-elections`                    | source repo used to resolve INE codes               |
| `PRESIDENTIAL_ELECTION_RESULTS_FILENAME` | `raw/presidential/PR_2026_Globais.xlsx`               | file downloaded from the repo above                 |
| `CITY_DATASET_CONFIG_NAME`               | `cities`                                              | HF dataset config name                              |

**`extract_geo/sub_flows/extract_districts/.env`**

| Variable                      | Default     | Notes               |
|--------------------------------|-------------|----------------------|
| `DISTRICT_DATASET_CONFIG_NAME` | `districts` | HF dataset config name |

## Running

From the repo root, with the venv active:

```bash
# Full pipeline (recommended): cities, then districts, then both are published
python -m minha_regiao.flows.extract_geo.ExtractGeo

# Or run one sub-flow at a time, e.g. while iterating on the cities scraper
python -m minha_regiao.flows.extract_geo.sub_flows.extract_cities.ExtractCities
python -m minha_regiao.flows.extract_geo.sub_flows.extract_districts.ExtractDistricts
```

Each is a regular Prefect `@flow`, so it also runs via `prefect deployment`
or by importing the flow function and calling it directly (e.g. from a
notebook or another flow) — that's how `ExtractGeo.py` itself invokes the
two sub-flows.
