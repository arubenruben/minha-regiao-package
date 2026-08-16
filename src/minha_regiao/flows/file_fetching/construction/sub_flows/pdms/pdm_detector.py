from .url_normalizer import URLNormalizer


class PDMDetector:
    KEYWORDS = ("PDM", "plano diretor municipal")

    def __init__(self):
        """Initialize the PDM detector with normalized keywords."""
        self.normalized_keywords = tuple(
            URLNormalizer.normalize_for_keyword_match(kw) for kw in self.KEYWORDS
        )

    def is_pdm_pdf_url(self, url: str, href_text: str = "") -> bool:
        if not url.lower().endswith(".pdf"):
            return False

        # Normalize URL and href text for keyword matching
        url_normalized = URLNormalizer.normalize_for_keyword_match(url)
        href_normalized = URLNormalizer.normalize_for_keyword_match(href_text)

        # Check if any PDM keyword appears in the URL or href text
        return any(
            keyword in url_normalized or keyword in href_normalized
            for keyword in self.normalized_keywords
        )

    def get_keywords(self) -> tuple[str, ...]:
        return self.KEYWORDS

    def get_normalized_keywords(self) -> tuple[str, ...]:
        return self.normalized_keywords
