from pathlib import Path

from prefect import flow


@flow(name="parse_regional_elections", description="Parse regional election result files.")
def regional_elections(paths: list[Path]) -> None:
    pass


if __name__ == "__main__":
    regional_elections([])
