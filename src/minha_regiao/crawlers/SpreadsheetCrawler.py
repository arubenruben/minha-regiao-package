import time
import tempfile
import requests
from tqdm import tqdm
from typing import List
from environs import Env
from bs4 import BeautifulSoup
from huggingface_hub import login
from minha_regiao.dto.Election import Election
from minha_regiao.crawlers.Crawler import Crawler
from minha_regiao.dto.ElectionFile import ElectionFile
from huggingface_hub import file_exists, upload_file, repo_exists, create_repo
from minha_regiao.dto.elections.PresidentialElection import PresidentialElection
from minha_regiao.dto.elections.MunicipalityElection import MunicipalityElection


class SpreadsheetCrawler(Crawler):

    def __init__(self, base_url: str, hf_api_key: str, hf_repo_name: str) -> None:
        super().__init__()

        self.base_url = base_url
        self.hf_api_key = hf_api_key
        self.hf_repo_name = hf_repo_name

        login(self.hf_api_key)

    def _extract_presidential_elections(self) -> List[PresidentialElection]:

        presidential_elections = []

        for year in tqdm(
            range(1974, time.localtime().tm_year + 1),
            desc="Presidential Elections",
            leave=False,
        ):
            base_file = f"{self.base_url}PresidenciaRepublica/Documents/PR_{year}"
            for ext in [".xlsx", ".xls"]:
                file_url = f"{base_file}{ext}"
                response = requests.get(file_url)

                # Check if it's actually a file download (not a redirect to HTML page)
                content_type = response.headers.get("Content-Type", "")

                if not response.status_code == 200:
                    continue

                if not (
                    "application/vnd" in content_type
                    or "application/octet-stream" in content_type
                ):
                    continue

                # TODO: Introduce logic for first and second round files
                # Currently only single round elections are handled

                unique_round_election_file = ElectionFile.from_url(
                    url=file_url,
                    file_format=ext.lstrip("."),
                )

                election = PresidentialElection(
                    year=year, url=file_url, first_round_file=unique_round_election_file
                )

                presidential_elections.append(election)

                break
            else:
                print(f"No spreadsheet found for presidential election in {year}")
                continue

        if not presidential_elections:
            raise ValueError("No presidential elections found.")

        return presidential_elections

    def _extract_municipal_elections(self) -> List[MunicipalityElection]:
        municipality_elections = []

        for year in range(1976, time.localtime().tm_year + 1):
            base_file_cm = f"{self.base_url}AutarquiasLocais/Documents/Autarquicas-{year}/resultados_eleicoes_CM_{year}"
            base_file_am = f"{self.base_url}AutarquiasLocais/Documents/Autarquicas-{year}/resultados_eleicoes_AM_{year}"
            base_file_af = f"{self.base_url}AutarquiasLocais/Documents/Autarquicas-{year}/resultados_eleicoes_AF_{year}"

            # Try to find each file independently with its own extension
            file_cm = None
            file_am = None
            file_af = None

            # Find CM file
            for ext in [".xlsx", ".xls"]:
                url = f"{base_file_cm}{ext}"
                response = requests.get(url)
                content_type = response.headers.get("Content-Type", "")

                if response.status_code == 200 and (
                    "application/vnd" in content_type
                    or "application/octet-stream" in content_type
                ):
                    file_cm = ElectionFile.from_url(
                        url=url,
                        file_format=ext.lstrip("."),
                    )
                    file_url_cm = url
                    break

            # Find AM file
            for ext in [".xlsx", ".xls"]:
                url = f"{base_file_am}{ext}"
                response = requests.get(url)
                content_type = response.headers.get("Content-Type", "")

                if response.status_code == 200 and (
                    "application/vnd" in content_type
                    or "application/octet-stream" in content_type
                ):
                    file_am = ElectionFile.from_url(
                        url=url,
                        file_format=ext.lstrip("."),
                    )
                    break

            # Find AF file
            for ext in [".xlsx", ".xls"]:
                url = f"{base_file_af}{ext}"
                response = requests.get(url)
                content_type = response.headers.get("Content-Type", "")

                if response.status_code == 200 and (
                    "application/vnd" in content_type
                    or "application/octet-stream" in content_type
                ):
                    file_af = ElectionFile.from_url(
                        url=url,
                        file_format=ext.lstrip("."),
                    )
                    break

            # Check if all three files were found
            if not file_cm or not file_am or not file_af:
                print(
                    f"No complete set of spreadsheets found for municipal election in {year}"
                )
                continue

            municipality_elections.append(
                MunicipalityElection(
                    year=year,
                    url="",
                    municipal_assembly_file=file_am,
                    parish_assembly_file=file_af,
                    town_hall_file=file_cm,
                )
            )

        return municipality_elections

    def _extract_parliamentary_elections(self):
        pass

    def extract(self) -> List[Election]:
        soup = BeautifulSoup(
            requests.get(f"{self.base_url}/Paginas/default.aspx").content,
            features="html.parser",
        )

        # Find the <div> with class 'subsites'
        subsites_div = soup.find("div", class_="subsites")

        if not subsites_div:
            raise ValueError("Could not find the 'subsites' div on the page")

        # List all <a> elements within this div
        list_items = subsites_div.find_all("a")

        # Presidência da República
        # Find the link whose text contains 'Presidência da República'
        presidential_link = next(
            (a["href"] for a in list_items if "Presidência da República" in a.text),
            None,
        )

        if not presidential_link:
            raise ValueError("Could not find the link for 'Presidência da República'")

        # Assembleia da República
        parliamentary_link = next(
            (a["href"] for a in list_items if "Assembleia da República" in a.text), None
        )

        if not parliamentary_link:
            raise ValueError("Could not find the link for 'Assembleia da República'")

        # Autarquias Locais
        municipal_link = next(
            (a["href"] for a in list_items if "Autarquias Locais" in a.text), None
        )

        if not municipal_link:
            raise ValueError("Could not find the link for 'Autarquias Locais'")

        # Parlamento Europeu
        european_parliament_link = next(
            (a["href"] for a in list_items if "Parlamento Europeu" in a.text), None
        )

        if not european_parliament_link:
            raise ValueError("Could not find the link for 'Parlamento Europeu'")

        presidential_elections = self._extract_presidential_elections()
        # municipal_elections = self._extract_municipal_elections()
        # parliamentary_elections = self._extract_parliamentary_elections()

        # TODO: Implement the extraction for the remaining election types
        # Regionais
        # Referendos
        # Autárquicas Intercalares
        # Conselho das Comunidades Portuguesas
        # Histórico de Eleições

        # TODO: Deal with the spreadsheet data returned

        return presidential_elections

    def transform(self, elections: List[Election]) -> List[Election]:
        return elections

    # Add Files to HF Hub if not already present
    def load(self, elections: List[Election]) -> None:
        # Verify if the repo exists and create it if not
        if not repo_exists(self.hf_repo_name, repo_type="dataset"):
            create_repo(
                repo_id=self.hf_repo_name,
                repo_type="dataset",
                private=False,
                exist_ok=True,
            )

        for election in tqdm(
            elections, desc="Loading Election Files to HF Hub", leave=False
        ):
            for file in election.get_election_files():
                if not file.hf_file_id:
                    raise ValueError(
                        f"hf_file_id is not set for file in election {election.__class__.__name__} for year {election.year}"
                    )

                if file_exists(
                    filename=file.hf_file_id,
                    repo_id=self.hf_repo_name,
                    repo_type="dataset",
                ):
                    print(
                        f"File {file.hf_file_id} already exists in the repository. Skipping upload."
                    )
                    continue

                upload_file(
                    path_or_fileobj=file.filepath,
                    path_in_repo=file.hf_file_id,
                    repo_id=self.hf_repo_name,
                    repo_type="dataset",
                    commit_message=f"Add {file.hf_file_id} election data for {election.year}",
                )

                print(f"Uploaded {file.hf_file_id} to HF Hub.")


if __name__ == "__main__":
    env = Env()
    env.read_env(override=True)

    crawler = SpreadsheetCrawler(
        base_url="https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/",
        hf_api_key=env.str("HF_API_KEY"),
        hf_repo_name=env.str("HF_FILE_REPO"),
    )

    elections = crawler.extract()
    transformed_elections = crawler.transform(elections)
    crawler.load(transformed_elections)
