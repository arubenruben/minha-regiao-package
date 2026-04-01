import os
from typing import Optional
from bs4 import BeautifulSoup
from prefect import flow, task


@task(name="Fetch Town Hall List")
def fetch_town_hall_list(filepath: str) -> list[str]:
    with open(filepath, "r", encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")
    town_halls = []

    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag.get("href", ""))
        if href.startswith("http://www.cm-") or href.startswith("https://www.cm-"):
            town_halls.append(href)

    for a_tag in soup.find_all("a", href=True):
        text = a_tag.get_text(strip=True).lower()
        href = str(a_tag["href"]).replace("\t", "").replace("\n", "")
        if "consultar website" in text:
            town_halls.append(href)

    unique_town_halls = sorted(set(town_halls))
    unique_town_halls = [
        url.replace("http://", "https://") for url in unique_town_halls
    ]

    for url in [
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
    ]:
        if url not in unique_town_halls:
            unique_town_halls.append(url)

    if "https://Http://www.cm-campo-maior.pt" in unique_town_halls:
        unique_town_halls.remove("https://Http://www.cm-campo-maior.pt")
        unique_town_halls.append("https://www.cm-campo-maior.pt")

    assert (
        len(unique_town_halls) == 308
    ), f"Expected 308 town hall URLs, but found {len(unique_town_halls)}"

    return unique_town_halls


@flow(name="Fetch Town Halls")
def fetch_town_halls(data_dir: Optional[str] = None) -> list[str]:

    if data_dir is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        construction_dir = os.path.dirname(current_dir)
        data_dir = os.path.join(construction_dir, "data")

    filepath = os.path.join(data_dir, "link.html")

    return fetch_town_hall_list(filepath=filepath)
