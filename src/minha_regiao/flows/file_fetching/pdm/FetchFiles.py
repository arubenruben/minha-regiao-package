import requests
from tqdm import tqdm
from pypdf import PdfReader
from tempfile import NamedTemporaryFile
from minha_regiao.llm.Gemini import Gemini
from prefect import flow, task, get_run_logger
from minha_regiao.flows.file_fetching.pdm.Settings import Settings

settings = Settings()
    
@task(name="Fetch List of Municipalities Emails")
def fetch_list_municipalities_emails():
    response = requests.get("https://www.dgav.pt/wp-content/uploads/2022/03/Lista_Camaras-Municipais.pdf")

    with NamedTemporaryFile(delete=True) as tmp_file:
        tmp_file.write(response.content)
        tmp_file_path = tmp_file.name
        reader = PdfReader(tmp_file_path)

        for page in tqdm(reader.pages, desc="Extracting emails from PDF"):
            pass


    # Implement logic to extract emails from the PDF file
    # This could involve using a library like PyPDF2 or pdfplumber to read the
    

@flow(name="Fetch PDM Files")
def fetch_pdm_files():
    logger = get_run_logger()
    logger.info("Starting to fetch PDM files")

    fetch_list_municipalities_emails()
    
    # Implement the logic to fetch PDM files from the source
    # This could involve web scraping, API calls, or downloading from a known URL
    
    logger.info("Successfully fetched PDM files")

if __name__ == "__main__":
    fetch_pdm_files()