from pathlib import Path

from prefect import flow


@flow(name="parse_parliament_elections", description="Parse parliament election result files.")
def parliament_elections(paths: list[Path]) -> None:
    # TODO: not implemented yet. Once parsing/persistence lands here, wire
    # the output step onto minha_regiao.loader (Loader/DatabaseLoader/
    # JsonFileLoader), gated by this sub-flow's Settings.load_targets --
    # see extract_pdms.ExtractPDM / extract_rmues.ExtractRMUEs for the
    # reference pattern (documented in flows/CLAUDE.md).
    pass


if __name__ == "__main__":
    parliament_elections([])
