from huggingface_hub import HfApi
from prefect import get_run_logger, task


@task(name="ensure_dataset_repo")
def ensure_dataset_repo(token: str | None, repo_id: str) -> None:
    logger = get_run_logger()
    logger.info(f"Ensuring Hugging Face dataset repo {repo_id} exists")

    api = HfApi(token=token)
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True, private=False)
