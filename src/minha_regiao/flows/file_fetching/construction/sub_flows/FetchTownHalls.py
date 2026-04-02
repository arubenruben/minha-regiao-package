import os
from typing import Optional
from bs4 import BeautifulSoup
from prefect import flow, task
from minha_regiao.flows.file_fetching.construction.schema.TownHallDTO import TownHallDTO


def extract_domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    return parsed.netloc


@task(name="Fetch Town Hall List")
def fetch_town_hall_list(filepath: str) -> list[TownHallDTO]:
    with open(filepath, "r", encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")
    town_halls = []

    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag.get("href", ""))
        if href.startswith("http://www.cm-") or href.startswith("https://www.cm-"):
            town_halls.append(
                TownHallDTO(url=href, domain=extract_domain_from_url(href))
            )

    for a_tag in soup.find_all("a", href=True):
        text = a_tag.get_text(strip=True).lower()
        href = str(a_tag["href"]).replace("\t", "").replace("\n", "")
        if "consultar website" in text:
            town_halls.append(
                TownHallDTO(url=href, domain=extract_domain_from_url(href))
            )

    # Deduplicate by URL
    seen_urls = set()
    unique_town_halls = []
    for th in town_halls:
        if th.url not in seen_urls:
            seen_urls.add(th.url)
            unique_town_halls.append(th)

    # Normalize URLs to HTTPS
    for th in unique_town_halls:
        th.url = th.url.replace("http://", "https://")

    # Add known missing town halls
    known_town_halls = [
        "https://www.mogadouro.pt/",
        "https://www.cm-maia.pt/",
        "https://www.ourem.pt/",
        "https://www.sines.pt/",
        "https://www.cmav.pt/",
        "https://www.cmpb.pt/",
        "https://www.chaves.pt/",
        "https://valpacos.pt/",
        "https://angradoheroismo.pt/",
        "https://www.cmpv.pt/",
        "https://cmvfc.pt/",
    ]

    for url in known_town_halls:
        if url not in seen_urls:
            unique_town_halls.append(
                TownHallDTO(url=url, domain=extract_domain_from_url(url))
            )
            seen_urls.add(url)

    # Fix malformed URL
    malformed_url = "https://Http://www.cm-campo-maior.pt"
    
    if malformed_url in seen_urls:
        seen_urls.remove(malformed_url)
        unique_town_halls = [
            th for th in unique_town_halls if th.url != malformed_url
        ]
        unique_town_halls.append(
            TownHallDTO(
                url="https://www.cm-campo-maior.pt",
                domain="www.cm-campo-maior.pt",
            )
        )

    assert (
        len(unique_town_halls) == 308
    ), f"Expected 308 town hall URLs, but found {len(unique_town_halls)}"

    return unique_town_halls


@flow(name="Fetch Town Halls")
def fetch_town_halls(data_dir: Optional[str] = None) -> list[TownHallDTO]:

    if data_dir is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        construction_dir = os.path.dirname(current_dir)
        data_dir = os.path.join(construction_dir, "data")

    filepath = os.path.join(data_dir, "link.html")

    return fetch_town_hall_list(filepath=filepath)
