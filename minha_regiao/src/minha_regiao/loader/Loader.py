from typing import Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

RecordT = TypeVar("RecordT", bound=BaseModel, contravariant=True)


@runtime_checkable
class Loader(Protocol[RecordT]):
    """Common interface for every "load"/sink step a flow runs at the end
    of its pipeline (database persistence, a local file, a Hugging Face
    dataset publish, ...). Each flow builds its active loader list from its
    own `load_targets` setting and calls `load()` on each one -- typically
    from its own Prefect task, so one sink failing doesn't hide another's
    error (see flows/CLAUDE.md).

    Deliberately minimal: a `Loader` only knows how to accept a batch of
    already-built records and persist/publish/write them somewhere. It is
    NOT a generic ORM mapper -- entity-specific mapping (e.g. building a
    `City` row from a `CityContacts` record) stays in each flow's own
    repository function; a `Loader` like `DatabaseLoader` just adapts that
    function to this interface.
    """

    async def load(self, records: list[RecordT]) -> None: ...
