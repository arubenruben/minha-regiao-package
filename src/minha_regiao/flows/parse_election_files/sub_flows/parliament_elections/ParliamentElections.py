from pathlib import Path

from prefect import flow


@flow(name="parse_parliament_elections", description="Parse parliament election result files.")
def parliament_elections(paths: list[Path]) -> None:
    pass


if __name__ == "__main__":
    parliament_elections([])
