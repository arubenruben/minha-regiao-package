from pathlib import Path

from huggingface_hub import hf_hub_download


def download_election_file(repo_id: str, path_in_repo: str, hf_api_key: str) -> Path:
    return Path(
        hf_hub_download(
            repo_id=repo_id,
            filename=path_in_repo,
            repo_type="dataset",
            token=hf_api_key,
        )
    )
