from pathlib import Path

from prefect import flow


@flow(name="parse_european_elections", description="Parse european election result files.")
def european_elections(paths: list[Path]) -> None:
    pass


if __name__ == "__main__":
    european_elections([])
