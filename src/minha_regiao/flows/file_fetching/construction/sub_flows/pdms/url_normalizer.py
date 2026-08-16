import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit


class URLNormalizer:
    FILE_EXTENSIONS = frozenset(
        {
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
            ".zip",
            ".rar",
            ".7z",
            ".tar",
            ".gz",
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".bmp",
            ".svg",
            ".mp3",
            ".mp4",
            ".avi",
            ".mov",
            ".wmv",
            ".txt",
            ".csv",
            ".xml",
            ".json",
        }
    )

    @staticmethod
    def normalize_url(url: str) -> str:
        parts = urlsplit(url)
        scheme = parts.scheme.lower()
        hostname = (parts.hostname or "").lower()
        port = parts.port

        if port and (
            (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
        ):
            netloc = hostname
        elif port:
            netloc = f"{hostname}:{port}"
        else:
            netloc = hostname

        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        # Sort query params to treat same URLs with different query order as identical.
        query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
        return urlunsplit((scheme, netloc, path, query, ""))

    @staticmethod
    def normalize_for_keyword_match(value: str) -> str:
        return re.sub(r"[^0-9a-zA-Z]+", "", value).lower()

    @staticmethod
    def is_within_root_scope(
        candidate_url: str, root_domain: str, root_path: str
    ) -> bool:
        parsed = urlparse(candidate_url)

        if parsed.scheme not in {"http", "https"}:
            return False

        if parsed.netloc != root_domain:
            return False

        candidate_path = parsed.path or "/"
        if candidate_path != "/":
            candidate_path = candidate_path.rstrip("/")

        if root_path == "/":
            return True

        return candidate_path == root_path or candidate_path.startswith(f"{root_path}/")

    @classmethod
    def is_file_url(cls, url: str) -> bool:
        parsed = urlparse(url)
        path = parsed.path.lower()
        return any(path.endswith(ext) for ext in cls.FILE_EXTENSIONS)
