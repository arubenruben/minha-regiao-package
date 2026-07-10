from prefect import flow, task, get_run_logger


@task(name="hello_world_regional_elections")
def hello_world(url: str) -> str:
    return f"Hello world from the Regional Elections sub-flow! url={url}"


@flow(name="regional_elections")
def regional_elections(url: str) -> str:
    logger = get_run_logger()

    message = hello_world(url)
    logger.info(message)

    return message


if __name__ == "__main__":
    regional_elections("https://example.com")
