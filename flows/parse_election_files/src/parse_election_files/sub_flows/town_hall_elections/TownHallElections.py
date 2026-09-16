from pathlib import Path

from prefect import flow


@flow(name="parse_town_hall_elections", description="Parse town hall election result files.")
def town_hall_elections(paths: list[Path]) -> None:
    # TODO: not implemented yet. Once parsing/persistence lands here, wire
    # the output step onto minha_regiao.loader (Loader/DatabaseLoader/
    # JsonFileLoader), gated by this sub-flow's Settings.load_targets --
    # see extract_pdms.ExtractPDM / extract_rmues.ExtractRMUEs for the
    # reference pattern (documented in flows/CLAUDE.md).
    pass


if __name__ == "__main__":
    town_hall_elections([])
