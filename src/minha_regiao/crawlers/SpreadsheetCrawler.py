import time
import tempfile
import requests
from tqdm import tqdm
from typing import List
from environs import Env
from bs4 import BeautifulSoup
from huggingface_hub import login
from minha_regiao.crawlers.Crawler import Crawler
from huggingface_hub import file_exists, upload_file
from minha_regiao.dto.ElectionFile import ElectionFile


class SpreadsheetCrawler(Crawler):

    def __init__(self, base_url: str, hf_api_key: str, hf_repo_name: str) -> None:
        super().__init__()

        self.base_url = base_url
        self.hf_api_key = hf_api_key
        self.hf_repo_name = hf_repo_name

        login(self.hf_api_key)

    def _extract_presidential_elections(self) -> List[ElectionFile]:

        files = []

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

                with open(f"temp_presidential_{year}{ext}", "wb") as f:
                    f.write(response.content)

                file = ElectionFile.from_url(
                    url=file_url,
                    year=year,
                    election_type="presidential",
                    file_format=ext.lstrip("."),
                )

                files.append(file)

                break
            else:
                print(f"No spreadsheet found for presidential election in {year}")
                continue

        if not files:
            raise ValueError("No presidential election files were found.")

        return files

    def _extract_municipal_elections(self):
        pass

    def _extract_parliamentary_elections(self):
        pass

    def extract(self):
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

    def transform(self, files: List[ElectionFile]) -> List[ElectionFile]:
        # Convert the files from xls to xlsx if needed.
        parsed_files = []

        for file in tqdm(files, desc="Transforming Election Files", leave=False):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                df = file.get_df()
                df.to_excel(tmp_file.name, index=False)

                parsed_file = ElectionFile(
                    url=file.url,
                    year=file.year,
                    election_type=file.election_type,
                    filepath=tmp_file.name,
                    file_format="xlsx",
                )
                parsed_files.append(parsed_file)

        return parsed_files

    # Add Files to HF Hub if not already present
    def load(self, files: List[ElectionFile]) -> None:
        for file in tqdm(files, desc="Loading Election Files to HF Hub", leave=False):

            filename = f"elections/{file.election_type}_{file.year}.{file.file_format}"

            if file_exists(
                filename=filename, repo_id=self.hf_repo_name, repo_type="dataset"
            ):
                print(
                    f"File {filename} already exists in the repository. Skipping upload."
                )
                continue

            upload_file(
                path_or_fileobj=file.filepath,
                path_in_repo=filename,
                repo_id=self.hf_repo_name,
                repo_type="dataset",
                commit_message=f"Add {file.election_type} election data for {file.year}",
            )

            print(f"Uploaded {filename} to HF Hub.")


if __name__ == "__main__":
    env = Env()
    env.read_env(override=True)

    crawler = SpreadsheetCrawler(
        base_url="https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/",
        hf_api_key=env.str("HF_API_KEY"),
        hf_repo_name=env.str("HF_FILE_REPO"),
    )

    files = crawler.extract()
    files = crawler.transform(files)
    crawler.load(files)
