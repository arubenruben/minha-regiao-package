from pathlib import Path

from prefect import flow


@flow(name="parse_regional_elections", description="Parse regional election result files.")
def regional_elections(paths: list[Path]) -> None:
    # TODO: not implemented yet. Once parsing/persistence lands here, wire
    # the output step onto minha_regiao.loader (Loader/DatabaseLoader/
    # JsonFileLoader), gated by this sub-flow's Settings.load_targets --
    # see extract_pdms.ExtractPDM / extract_rmues.ExtractRMUEs for the
    # reference pattern (documented in flows/CLAUDE.md).
    pass


if __name__ == "__main__":
    regional_elections([])
