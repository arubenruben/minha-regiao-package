from prefect import flow, task, get_run_logger


@task(name="hello_world_town_hall_elections")
def hello_world(url: str) -> str:
    return f"Hello world from the Town Hall Elections sub-flow! url={url}"


@flow(name="town_hall_elections")
def town_hall_elections(url: str) -> str:
    logger = get_run_logger()

    message = hello_world(url)
    logger.info(message)

    return message


if __name__ == "__main__":
    town_hall_elections("https://example.com")
