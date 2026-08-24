from pathlib import Path

from datasets import Dataset, load_dataset
from prefect import flow, task, unmapped, get_run_logger

from parse_election_files.Settings import settings
from parse_election_files.services.ElectionFileDownloader import download_election_file


@task(name="load_election_dataset")
def load_election_dataset() -> Dataset:
    logger = get_run_logger()
    logger.info(
        f"Loading dataset {settings.hf_dataset_repo_id} (config: {settings.hf_dataset_config_name})"
    )

    dataset = load_dataset(
        settings.hf_dataset_repo_id,
        name=settings.hf_dataset_config_name,
        token=settings.hf_api_key,
        split="train",
    )

    logger.info(f"Loaded {len(dataset)} records from {settings.hf_dataset_repo_id}")
    return dataset


@task(name="download_election_file")
def download_election_file_task(repo_id: str, election_type: str, filename: str) -> Path:
    logger = get_run_logger()

    path_in_repo = f"raw/{election_type}/{filename}"
    logger.info(f"Downloading {path_in_repo} from {repo_id}")

    path = download_election_file(repo_id, path_in_repo, settings.hf_api_key)

    logger.info(f"Downloaded election file to {path}")
    return path


@flow(name="parse_elections", description="Parse structured election records from the raw election files dataset.")
def parse_elections() -> list[Path]:
    dataset = load_election_dataset()

    paths = download_election_file_task.map(
        unmapped(settings.hf_dataset_repo_id), dataset["type"], dataset["filename"]
    )

    return paths.result()


if __name__ == "__main__":
    parse_elections()
