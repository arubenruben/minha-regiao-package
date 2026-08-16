from pathlib import Path

from prefect import flow


@flow(name="parse_presidential_elections", description="Parse presidential election result files.")
def presidential_elections(paths: list[Path]) -> None:
    pass


if __name__ == "__main__":
    presidential_elections([])
