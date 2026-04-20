import os
import json
import asyncio
from tqdm import tqdm
from dataclasses import dataclass, field
from minha_regiao.schema.City import City
from prefect import flow, task, get_run_logger
from urllib.parse import urlparse, urljoin, urlunparse
from typing import Sequence, Set, List, Tuple, Optional, Any
from minha_regiao.scrapping.ScraperStrategy import ScraperStrategy

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = CURRENT_DIR.split("src")[0]
CACHE_DIR = os.path.join(ROOT_DIR, "cache")

PDM_KEYWORDS = [
    "pdm",
    "plano diretor municipal",
    "plano_diretor",
    "pdm_municipal",
    "plano-diretor",
    "estratégia municipal",
    "ordenamento do território",
]


def normalize_url(url: str) -> str:
    parsed = urlparse(url)

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"

    if path != "/":
        path = path.rstrip("/")

    # Remove fragment, keep query
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def is_pdm_file(url: str) -> bool:
    path = urlparse(url).path.lower()

    if not path.endswith(".pdf"):
        return False

    return any(keyword in path for keyword in PDM_KEYWORDS)


def is_file_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    last = path.split("/")[-1]

    if "." not in last:
        return False

    html_extensions = (".html", ".htm", ".php", ".asp", ".aspx", ".jsp")
    return not any(path.endswith(ext) for ext in html_extensions)


async def crawl_single_page(
    url: str,
    domain: str,
    scraper: ScraperStrategy,
    logger: Any,
) -> Tuple[List[str], List[str]]:
    try:
        logger.info(f"Crawling {url} for candidate files...")

        soup = await scraper.query(url)

        if soup is None:
            logger.warning(f"Received no content for {url}")
            return [], []

        same_domain_links: Set[str] = set()
        pdm_files: Set[str] = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if not isinstance(href, str):
                continue

            full_url = normalize_url(urljoin(url, href))
            parsed = urlparse(full_url)

            if parsed.netloc != domain:
                continue

            if is_pdm_file(full_url):
                logger.info(f"Found candidate PDM file: {full_url}")
                pdm_files.add(full_url)
            elif not is_file_url(full_url):
                same_domain_links.add(full_url)

        return list(same_domain_links), list(pdm_files)

    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return [], []


@dataclass
class CrawlState:
    domain: str
    scraper: ScraperStrategy
    logger: Any
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    seen: Set[str] = field(default_factory=set)
    found_files: Set[str] = field(default_factory=set)


async def crawl_worker(worker_id: int, state: CrawlState) -> None:
    while True:
        url: Optional[str] = await state.queue.get()

        if url is None:
            state.queue.task_done()
            return

        try:
            new_links, pdm_files = await crawl_single_page(
                url=url,
                domain=state.domain,
                scraper=state.scraper,
                logger=state.logger,
            )

            state.found_files.update(pdm_files)

            for link in new_links:
                if link not in state.seen:
                    state.seen.add(link)
                    await state.queue.put(link)

        except Exception as e:
            state.logger.error(f"Worker {worker_id} failed on {url}: {e}")
        finally:
            state.queue.task_done()


@task(name="Crawl Town Hall Website")
async def crawl_town_hall_website(
    town_hall_url: str,
    scraper: ScraperStrategy,
    max_concurrency: int = 128,
) -> Sequence[str]:
    logger = get_run_logger()

    start_url = normalize_url(town_hall_url)
    domain = urlparse(start_url).netloc.lower()

    state = CrawlState(
        domain=domain,
        scraper=scraper,
        logger=logger,
    )

    state.seen.add(start_url)
    await state.queue.put(start_url)

    workers = [
        asyncio.create_task(crawl_worker(i, state)) for i in range(max_concurrency)
    ]

    await state.queue.join()

    for _ in workers:
        await state.queue.put(None)

    await asyncio.gather(*workers)

    return sorted(state.found_files)


@task(name="Read Cache File")
async def read_cache_file(cache_path: str) -> Sequence[City]:
    if not os.path.exists(cache_path):
        return []

    with open(cache_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return [City.model_validate(item) for item in data]


@task(name="Write Cache File")
async def write_cache_file(cache_path: str, city: City) -> None:
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump([city.model_dump()], f, ensure_ascii=False, indent=2)


@flow(name="Fetch Candidate Files")
async def fetch_candidate_files(
    cities: Sequence[City],
    scraper: ScraperStrategy,
):
    logger = get_run_logger()
    logger.info("Fetching candidate files...")

    cached_cities = await read_cache_file(
        os.path.join(CACHE_DIR, "candidate_files.json")
    )
    # Filter out cities that are already cached
    cached_city_names = {city.name for city in cached_cities}
    cities_to_process = [city for city in cities if city.name not in cached_city_names]

    logger.info(
        f"Filtered out {len(cached_city_names)} cached cities, {len(cities_to_process)} remaining to process"
    )

    # sequential across cities
    for city in tqdm(cities_to_process):
        websites = await crawl_town_hall_website(
            town_hall_url=city.town_hall.website,
            scraper=scraper,
            max_concurrency=128,
        )

        if websites:
            logger.info(f"Found {len(websites)} candidate files for {city.name}")
        else:
            logger.info(f"No candidate files found for {city.name}")

        city.town_hall.pdm_candidate_files = websites

        # Update cache after each city to ensure progress is saved
        await write_cache_file(
            os.path.join(CACHE_DIR, "candidate_files.json"),
            city,
        )

        """
        websites
['http://www.cm-agueda.pt/cmagueda/uploads/document/file/1041/DA_revisao_PDM_Agueda.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/1814/Regulamento_Revisao_PDM_001.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/2063/3_PPPEC_Extracto_Carta_Condicionantes_PDM.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/2064/4_PPPEC_Extracto_Carta_REN_PDM.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/2068/2_PPPEC_Extracto_Planta_Ordenamento_PDM.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/4752/relatorioavaliacaocontrolo2019_aae_pdm.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6196/fundamentacao_termos_referencia_alt_pdm_pec_ic2.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6318/aviso_16460_2024_2_medidas_preventivas_suspensao_do_pdma.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6392/alteracao_por_adaptacao_d...forca_da_entrada_em_vigor_do_pgri_2024.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6973/aae_do_pdm___rac_2020_2023.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/5230/14_aviso_alteracao_pdma_encerramento_participacao.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7578/aviso_alt_pdm_pec_ic2.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7579/edital_alt_pdm_pec_ic2.pdf', 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/82/Plano_Diretor_Municipal_de__gueda.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/1041/DA_revisao_PDM_Agueda.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/1814/Regulamento_Revisao_PDM_001.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/2063/3_PPPEC_Extracto_Carta_Condicionantes_PDM.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/2064/4_PPPEC_Extracto_Carta_REN_PDM.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/2068/2_PPPEC_Extracto_Planta_Ordenamento_PDM.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/4752/relatorioavaliacaocontrolo2019_aae_pdm.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6196/fundamentacao_termos_referencia_alt_pdm_pec_ic2.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6318/aviso_16460_2024_2_medidas_preventivas_suspensao_do_pdma.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6392/alteracao_por_adaptacao_...forca_da_entrada_em_vigor_do_pgri_2024.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6973/aae_do_pdm___rac_2020_2023.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/5230/14_aviso_alteracao_pdma_encerramento_participacao.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7578/aviso_alt_pdm_pec_ic2.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7579/edital_alt_pdm_pec_ic2.pdf', 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/82/Plano_Diretor_Municipal_de__gueda.pdf']
special variables:
function variables:
00: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/1041/DA_revisao_PDM_Agueda.pdf'
01: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/1814/Regulamento_Revisao_PDM_001.pdf'
02: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/2063/3_PPPEC_Extracto_Carta_Condicionantes_PDM.pdf'
03: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/2064/4_PPPEC_Extracto_Carta_REN_PDM.pdf'
04: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/2068/2_PPPEC_Extracto_Planta_Ordenamento_PDM.pdf'
05: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/4752/relatorioavaliacaocontrolo2019_aae_pdm.pdf'
06: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6196/fundamentacao_termos_referencia_alt_pdm_pec_ic2.pdf'
07: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6318/aviso_16460_2024_2_medidas_preventivas_suspensao_do_pdma.pdf'
08: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6392/alteracao_por_adaptacao_do_pdm_de_agueda_por_forca_da_entrada_em_vigor_do_pgri_2024.pdf'
09: 'http://www.cm-agueda.pt/cmagueda/uploads/document/file/6973/aae_do_pdm___rac_2020_2023.pdf'
10: 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/5230/14_aviso_alteracao_pdma_encerramento_participacao.pdf'
11: 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7578/aviso_alt_pdm_pec_ic2.pdf'
12: 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7579/edital_alt_pdm_pec_ic2.pdf'
13: 'http://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/82/Plano_Diretor_Municipal_de__gueda.pdf'
14: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/1041/DA_revisao_PDM_Agueda.pdf'
15: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/1814/Regulamento_Revisao_PDM_001.pdf'
16: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/2063/3_PPPEC_Extracto_Carta_Condicionantes_PDM.pdf'
17: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/2064/4_PPPEC_Extracto_Carta_REN_PDM.pdf'
18: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/2068/2_PPPEC_Extracto_Planta_Ordenamento_PDM.pdf'
19: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/4752/relatorioavaliacaocontrolo2019_aae_pdm.pdf'
20: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6196/fundamentacao_termos_referencia_alt_pdm_pec_ic2.pdf'
21: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6318/aviso_16460_2024_2_medidas_preventivas_suspensao_do_pdma.pdf'
22: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6392/alteracao_por_adaptacao_do_pdm_de_agueda_por_forca_da_entrada_em_vigor_do_pgri_2024.pdf'
23: 'https://www.cm-agueda.pt/cmagueda/uploads/document/file/6973/aae_do_pdm___rac_2020_2023.pdf'
24: 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/5230/14_aviso_alteracao_pdma_encerramento_participacao.pdf'
25: 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7578/aviso_alt_pdm_pec_ic2.pdf'
26: 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/7579/edital_alt_pdm_pec_ic2.pdf'
27: 'https://www.cm-agueda.pt/cmagueda/uploads/writer_file/document/82/Plano_Diretor_Municipal_de__gueda.pdf'
len(): 28
        """
