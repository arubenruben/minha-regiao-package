import asyncio
import json

from prefect import flow, get_run_logger

from extract_pdms.schema.PDMRecord import PDMRecord
from extract_pdms.services.Logging import configure_file_logging
from extract_pdms.Settings import settings
from extract_pdms.sub_flows.extract_regulation_texts import extract_regulation_texts
from extract_pdms.sub_flows.find_pdms_in_snit import (
    find_pdms_in_snit,
    load_municipalities,
)

configure_file_logging(settings.logs_dir)


def _write_output(pdm_records: list[PDMRecord]) -> None:
    settings.output_file.parent.mkdir(parents=True, exist_ok=True)
    settings.output_file.write_text(
        json.dumps(
            [pdm_record.model_dump(mode="json") for pdm_record in pdm_records],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


@flow(
    name="extract_pdms",
    description=(
        "Resolve every municipality's PDM (Plano Diretor Municipal) via SNIT and extract "
        "the text of each regulation PDF in its history."
    ),
)
async def extract_pdms() -> list[PDMRecord]:
    logger = get_run_logger()

    municipalities = load_municipalities()
    pdm_records = await find_pdms_in_snit(municipalities)
    pdm_records = await extract_regulation_texts(pdm_records)

    _write_output(pdm_records)
    logger.info(f"Wrote {len(pdm_records)} PDM record(s) to {settings.output_file}")

    return pdm_records


if __name__ == "__main__":
    asyncio.run(extract_pdms())
