"""PDM (Plano Diretor Municipal) crawling and detection module."""
from .url_normalizer import URLNormalizer
from .pdm_detector import PDMDetector
from .web_crawler import WebCrawler
from .tasks import crawl_for_pdm_candidates

__all__ = [
    "URLNormalizer",
    "PDMDetector",
    "WebCrawler",
    "crawl_for_pdm_candidates",
]
