import os
import httpx
import pandas as pd
from typing import Sequence
from minha_regiao.schema.City import City
from prefect import flow, task, get_run_logger
from minha_regiao.schema.TownHall import TownHall

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = CURRENT_DIR.split("src")[0]
DATA_DIR = os.path.join(ROOT_DIR, "data")



@task(name="Retrieve Town Hall Data File")
def retrieve_town_hall_file(data_dir: str, filename: str, url: str) -> pd.DataFrame:
    logger = get_run_logger()

    filepath = os.path.join(data_dir, filename)

    if os.path.exists(filepath):
        logger.info(f"File found at {filepath}")
        return pd.read_excel(filepath, skiprows=2)
    
    logger.info(f"File not found at {filepath}. Downloading from {url}")
    response = httpx.get(url, follow_redirects=True)
    response.raise_for_status()
    
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name
    
    try:
        return pd.read_excel(tmp_path, skiprows=2)
    finally:
        os.remove(tmp_path)


@task(name="Validate Town Hall Header")
def validate_town_hall_header(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    
    # Normalize column names: strip leading/trailing whitespace, 
    # replace multiple internal whitespaces with a single one, and lowercase.
    df.columns = [
        " ".join(col.split()).lower() for col in df.columns
    ]
    
    expected_columns = [
        "distrito", "município", "cod.ine", "nif", "nome presidente", 
        "morada", "código postal", "e-mail", "telefone", "sitio"
    ]
    
    actual_columns = df.columns.tolist()
    
    print(f"Actual columns in the spreadsheet: {actual_columns}")

    # Check if all expected columns are present
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    
    if missing_columns:
        logger.error(f"Spreadsheet header is missing required columns: {missing_columns}")
        raise ValueError(f"Missing columns in spreadsheet: {missing_columns}")
    
    logger.info("Spreadsheet header validated successfully.")
    return df


@task(name="Parse Town Hall Data")
def parse_town_hall_data(df: pd.DataFrame) -> Sequence[City]:
    logger = get_run_logger()
    cities = []
    
    for _, row in df.iterrows():
        city_name = str(row["município"])
        district = str(row["distrito"])
        
        website = str(row["sitio"])
        if website.startswith("www."):
            website = f"https://{website}"
        
        town_hall = TownHall(
            website=website,
            nif=str(row["nif"]),
            president=str(row["nome presidente"]),
            address=str(row["morada"]),
            postal_code=str(row["código postal"]),
            email=str(row["e-mail"]),
            phone=str(row["telefone"])
        )
        
        cities.append(City(
            name=city_name,
            district=district,
            town_hall=town_hall
        ))

    if len(cities) != 308:
        logger.error(f"Expected 308 cities, but found {len(cities)}. Please check the source file for missing or extra entries.")
        raise ValueError(f"Expected 308 cities, but found {len(cities)}")
    
    return cities


@flow(name="Fetch Town Hall Websites")
def get_cities(
    filename: str = "contactos presidente CM 17032026.xlsx",
    url: str = "https://portalautarquico.dgal.gov.pt/ficheiros/?schema=f7664ca7-3a1a-4b25-9f46-2056eef44c33&channel=266f4a32-848e-4d2c-99c6-ad5b5511d7fe&content_id=EC39B430-9177-4A2E-94B9-6078F4704AD8&field=storage_image&lang=pt&ver=1&filetype=xlsx&dtestate=2026-03-17105724"
) -> Sequence[City]:
    
    df = retrieve_town_hall_file(DATA_DIR, filename, url)
    df = validate_town_hall_header(df)
    
    return parse_town_hall_data(df)

if __name__ == "__main__":
    cities = get_cities()

    for city in cities:
        print(f"{city.name} ({city.district}): {city.town_hall.website}")