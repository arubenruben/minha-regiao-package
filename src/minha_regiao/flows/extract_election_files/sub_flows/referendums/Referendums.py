from prefect import flow, task, get_run_logger


@task(name="hello_world_referendums")
def hello_world(url: str) -> str:
    return f"Hello world from the Referendums sub-flow! url={url}"


@flow(name="referendums")
def referendums(url: str) -> str:
    logger = get_run_logger()

    message = hello_world(url)
    logger.info(message)

    return message


if __name__ == "__main__":
    referendums("https://example.com")
