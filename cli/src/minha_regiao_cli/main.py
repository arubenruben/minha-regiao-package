import asyncio
from typing import Callable

import typer

app = typer.Typer(
    name="minha-regiao",
    help="Run minha-regiao's Prefect flows without memorizing each package's module path.",
    no_args_is_help=True,
)


def _run_extract_geo() -> None:
    from extract_geo.ExtractGeo import extract_geo

    extract_geo()


def _run_extract_pdms() -> None:
    from extract_pdms.ExtractPDM import extract_pdms

    asyncio.run(extract_pdms())


def _run_extract_rmues() -> None:
    from extract_rmues.ExtractRMUEs import extract_rmues
    from extract_rmues.Settings import settings

    asyncio.run(extract_rmues(base_url=settings.rmue_url))


def _run_extract_election_files() -> None:
    from extract_election_files.ElectionFiles import fetch_election_files

    asyncio.run(fetch_election_files())


def _run_parse_election_files() -> None:
    from parse_election_files.ParseElections import parse_elections

    parse_elections()


# Maps a CLI-facing flow name to a zero-argument callable that runs it
# in-process -- the same way each flow's own `if __name__ == "__main__":`
# block already does. Kept as a dict of thunks (not eagerly-imported flow
# functions) so `minha-regiao list` doesn't have to import every flow
# package's heavy dependencies (scrapling, playwright, ...) just to print
# their names.
_FLOWS: dict[str, Callable[[], None]] = {
    "extract-geo": _run_extract_geo,
    "extract-pdms": _run_extract_pdms,
    "extract-rmues": _run_extract_rmues,
    "extract-election-files": _run_extract_election_files,
    "parse-election-files": _run_parse_election_files,
}


@app.command("list")
def list_flows() -> None:
    """Print the flow names accepted by `minha-regiao run <name>`."""
    for name in _FLOWS:
        typer.echo(name)


@app.command("run")
def run_flow(name: str) -> None:
    """Run a flow in-process, by name (see `minha-regiao list`)."""
    flow = _FLOWS.get(name)
    if flow is None:
        valid = ", ".join(_FLOWS)
        typer.echo(f"Unknown flow '{name}'. Valid names: {valid}", err=True)
        raise typer.Exit(code=1)

    flow()


if __name__ == "__main__":
    app()
