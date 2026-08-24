from pathlib import Path

from huggingface_hub import hf_hub_download


def download_dataset_file(repo_id: str, filename: str, hf_api_key: str | None) -> Path:
    return Path(
        hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
            token=hf_api_key or None,
        )
    )
