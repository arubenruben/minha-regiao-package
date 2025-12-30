import time
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from minha_regiao.crawlers.Crawler import Crawler

BASE_URL = "https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/"


# https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/PresidenciaRepublica/Documents/PR_1976.xlsx
class SpreadsheetCrawler(Crawler):

    def _extract_presidential_elections(self):
        for year in tqdm(
            range(1974, time.localtime().tm_year + 1),
            desc="Presidential Elections",
            leave=False,
        ):
            # Verify if exists a link https://www.sg.mai.gov.pt/AdministracaoEleitoral/EleicoesReferendos/PresidenciaRepublica/Documents/PR_1976.xlsx
            xlsx_file = f"{BASE_URL}/PresidenciaRepublica/Documents/PR_{year}.xlsx"

            reqsponse = requests.head(xlsx_file)

            if reqsponse.status_code == 200:
                pass
            else:
                # Try with .xls extension
                xls_file = f"{BASE_URL}/PresidenciaRepublica/Documents/PR_{year}.xls"

                if not requests.head(xls_file).status_code == 200:
                    raise ValueError(
                        f"Could not find presidential election data for year {year}"
                    )

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
