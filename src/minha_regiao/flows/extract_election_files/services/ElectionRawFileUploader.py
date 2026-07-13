import logging

import httpx
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_url

from minha_regiao.flows.extract_election_files.schema.StructuredElection import StructuredElection

logger = logging.getLogger(__name__)


class ElectionRawFileUploader:
    """Uploads the raw election result files (xls/xlsx) into a Hugging Face dataset repo,
    returning each election with `raw_file_url` filled in so it can be added to the tabular data.

    A dataset repo is a git repo under the hood, so every file is staged as a
    single commit instead of one push per file — that keeps history sane and
    avoids a commit/lock round-trip per file.
    """

    def __init__(self, api: HfApi, repo_id: str, raw_files_dir: str = "raw"):
        self._api = api
        self._repo_id = repo_id
        self._raw_files_dir = raw_files_dir

    def upload(self, elections: list[StructuredElection]) -> list[StructuredElection]:
        paths_in_repo = [self._path_in_repo(election) for election in elections]

        operations = [
            CommitOperationAdd(path_in_repo=path, path_or_fileobj=self._download(election.url))
            for election, path in zip(elections, paths_in_repo)
        ]

        logger.info(f"Uploading {len(operations)} raw election files to {self._repo_id}")
        self._api.create_commit(
            repo_id=self._repo_id,
            repo_type="dataset",
            operations=operations,
            commit_message=f"Add {len(operations)} raw election result files",
        )

        return [
            election.model_copy(
                update={"raw_file_url": hf_hub_url(repo_id=self._repo_id, filename=path, repo_type="dataset")}
            )
            for election, path in zip(elections, paths_in_repo)
        ]

    def _path_in_repo(self, election: StructuredElection) -> str:
        return f"{self._raw_files_dir}/{election.type}/{election.filename}"

    def _download(self, url: str) -> bytes:
        response = httpx.get(url, follow_redirects=True, timeout=60)
        response.raise_for_status()
        return response.content
