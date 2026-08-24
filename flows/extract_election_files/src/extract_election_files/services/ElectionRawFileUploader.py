import asyncio
import logging

import httpx
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_url

from extract_election_files.schema.StructuredElection import StructuredElection

logger = logging.getLogger(__name__)


class ElectionRawFileUploader:
    """Uploads the raw election result files (xls/xlsx) into a Hugging Face dataset repo,
    returning each election with `raw_file_url` filled in so it can be added to the tabular data.

    A dataset repo is a git repo under the hood, so every file is staged as a
    single commit instead of one push per file — that keeps history sane and
    avoids a commit/lock round-trip per file.
    """

    _MAX_RETRIES = 3
    _RETRY_DELAY_SECONDS = 5
    _DOWNLOAD_TIMEOUT_SECONDS = 180
    _MAX_CONCURRENT_DOWNLOADS = 5

    def __init__(self, api: HfApi, repo_id: str, raw_files_dir: str = "raw"):
        self._api = api
        self._repo_id = repo_id
        self._raw_files_dir = raw_files_dir

    async def upload(self, elections: list[StructuredElection]) -> list[StructuredElection]:
        paths_in_repo = [self._path_in_repo(election) for election in elections]

        semaphore = asyncio.Semaphore(self._MAX_CONCURRENT_DOWNLOADS)
        contents: list[bytes | None] = [None] * len(elections)
        async with httpx.AsyncClient(follow_redirects=True, timeout=self._DOWNLOAD_TIMEOUT_SECONDS) as client:
            async with asyncio.TaskGroup() as task_group:
                for index, election in enumerate(elections):
                    task_group.create_task(self._download_into(contents, index, client, semaphore, election.url))

        if any(content is None for content in contents):
            raise RuntimeError("Failed to download all election files")

        operations = [
            CommitOperationAdd(path_in_repo=path, path_or_fileobj=content)
            for content, path in zip(contents, paths_in_repo)
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

    async def _download_into(
        self,
        contents: list[bytes | None],
        index: int,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
    ) -> None:
        contents[index] = await self._download(client, semaphore, url)

    def _path_in_repo(self, election: StructuredElection) -> str:
        return f"{self._raw_files_dir}/{election.type}/{election.filename}"

    async def _download(self, client: httpx.AsyncClient, semaphore: asyncio.Semaphore, url: str) -> bytes:
        async with semaphore:
            for attempt in range(1, self._MAX_RETRIES + 1):
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.content
                except (httpx.TransportError, httpx.HTTPStatusError) as error:
                    if isinstance(error, httpx.HTTPStatusError) and not self._is_retryable_status(error):
                        raise
                    if attempt == self._MAX_RETRIES:
                        raise
                    logger.warning(
                        f"Transient error downloading {url}, retrying in {self._RETRY_DELAY_SECONDS}s "
                        f"(attempt {attempt}/{self._MAX_RETRIES})"
                    )
                    await asyncio.sleep(self._RETRY_DELAY_SECONDS)

        raise AssertionError("unreachable")  # loop always returns or raises

    @staticmethod
    def _is_retryable_status(error: httpx.HTTPStatusError) -> bool:
        return error.response.status_code == 429 or 500 <= error.response.status_code < 600
