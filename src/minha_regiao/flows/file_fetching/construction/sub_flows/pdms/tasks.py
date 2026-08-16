from .web_crawler import WebCrawler
from minha_regiao.flows.file_fetching.construction.schema.PDMCrawlResultDTO import (
    PDMCrawlResultDTO,
)


async def crawl_for_pdm_candidates(
    root_url: str,
    max_concurrency: int = 12,
    max_pages: int = 400,
    logger=None,
) -> PDMCrawlResultDTO:
    if logger:
        logger.info(f"Starting PDM crawl for: {root_url}")

    crawler = WebCrawler(root_url, max_concurrency, max_pages, logger)
    result = await crawler.crawl()

    if logger:
        logger.info(
            f"Crawl completed: visited {result.pages_visited} pages, "
            f"found {len(result.candidate_pdf_urls)} PDM candidates"
        )

    return result
