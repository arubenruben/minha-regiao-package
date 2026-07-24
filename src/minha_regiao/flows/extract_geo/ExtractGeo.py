from prefect import flow, get_run_logger

from minha_regiao.flows.extract_geo.sub_flows.extract_cities.ExtractCities import extract_cities
from minha_regiao.flows.extract_geo.sub_flows.extract_districts.ExtractDistricts import extract_districts


@flow(
    name="extract_geo",
    description="Extract cities and districts and publish the combined geo dataset to Hugging Face.",
)
def extract_geo() -> None:
    logger = get_run_logger()

    contacts = extract_cities()
    logger.info(f"Extracted {len(contacts)} cities")

    updated = extract_districts()
    logger.info(f"Assigned districts to {updated} cities")


if __name__ == "__main__":
    extract_geo()
