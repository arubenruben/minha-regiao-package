import time
import requests
import pandas as pd
from tqdm import tqdm
from typing import List
from bs4 import BeautifulSoup
from minha_regiao.crawlers.Crawler import Crawler
from minha_regiao.dto.ElectionFile import ElectionFile

BASE_URL = "https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/"


# https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/PresidenciaRepublica/Documents/PR_1976.xlsx
class SpreadsheetCrawler(Crawler):

    def _extract_presidential_elections(self) -> List[ElectionFile]:

        for year in tqdm(
            range(1974, time.localtime().tm_year + 1),
            desc="Presidential Elections",
            leave=False,
        ):
            base_file = f"{BASE_URL}PresidenciaRepublica/Documents/PR_{year}"

            for ext in [".xlsx", ".xls"]:
                file_url = f"{base_file}{ext}"
                response = requests.get(file_url)

                # Check if it's actually a file download (not a redirect to HTML page)
                content_type = response.headers.get("Content-Type", "")

                if (
                    "application/vnd" in content_type
                    or "application/octet-stream" in content_type
                ):
                    with open(f"temp_presidential_{year}{ext}", "wb") as f:
                        f.write(response.content)

                    excel_file = pd.ExcelFile(f"temp_presidential_{year}{ext}")

                    for sheet_name in excel_file.sheet_names:
                        file = ElectionFile(
                            url=file_url,
                            year=year,
                            election_type="presidential",
                        )
                    break
            else:
                print(f"No spreadsheet found for presidential election in {year}")
                continue

            # print(df.head())

        raise NotImplementedError("Subclasses must implement this method")

    def _extract_municipal_elections(self):
        pass

    def _extract_parliamentary_elections(self):
        pass

    def extract(self):
        soup = BeautifulSoup(
            requests.get(f"{BASE_URL}/Paginas/default.aspx").content,
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
        municipal_elections = self._extract_municipal_elections()
        parliamentary_elections = self._extract_parliamentary_elections()

        # TODO: Implement the extraction for the remaining election types
        # Regionais
        # Referendos
        # Autárquicas Intercalares
        # Conselho das Comunidades Portuguesas
        # Histórico de Eleições

        # TODO: Deal with the spreadsheet data returned

        raise NotImplementedError("Subclasses must implement this method")

    def transform(self):
        raise NotImplementedError("Subclasses must implement this method")

    def load(self):
        raise NotImplementedError("Subclasses must implement this method")


if __name__ == "__main__":
    crawler = SpreadsheetCrawler()
    crawler.extract()
