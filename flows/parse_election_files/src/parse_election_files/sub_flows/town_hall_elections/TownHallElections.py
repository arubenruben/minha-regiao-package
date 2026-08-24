from pathlib import Path

from prefect import flow


@flow(name="parse_town_hall_elections", description="Parse town hall election result files.")
def town_hall_elections(paths: list[Path]) -> None:
    pass


if __name__ == "__main__":
    town_hall_elections([])
