from typing import Awaitable, Callable, Generic, TypeVar

from pydantic import BaseModel

RecordT = TypeVar("RecordT", bound=BaseModel)


class DatabaseLoader(Generic[RecordT]):
    """Adapts an existing flow-specific `persist_*` repository function
    (e.g. `CityRepository.persist_cities`, `RMURepository.persist_rmue_regulations`)
    to the common `Loader` interface. It does NOT attempt to be a generic
    ORM mapper -- the injected `persist` callable keeps doing the
    entity-specific `update_or_create` mapping; this class only adapts its
    signature so callers can treat every sink (database, JSON file,
    Hugging Face, ...) the same way.

    `persist` may return anything (most repository functions return an
    int count, some return a richer tuple) -- `DatabaseLoader` doesn't
    inspect the result, so callers that need it (e.g. to log an unmatched
    count) should wrap `persist` in a small adapter before injecting it.
    """

    def __init__(self, persist: Callable[[list[RecordT]], Awaitable[object]]) -> None:
        self._persist = persist

    async def load(self, records: list[RecordT]) -> None:
        await self._persist(records)
