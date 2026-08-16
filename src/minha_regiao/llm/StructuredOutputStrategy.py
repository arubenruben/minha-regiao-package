import asyncio
from abc import ABC, abstractmethod
from typing import TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class StructuredOutputStrategy(ABC):
    """Common interface for LLM backends that turn a prompt into a validated Pydantic model.
    Concrete strategies (Google, OpenAI-compatible, ...) are interchangeable behind this type."""

    @abstractmethod
    async def generate(self, prompt: str, response_model: type[T]) -> T: ...

    async def generate_batch(self, prompts: list[str], response_model: type[T]) -> list[T]:
        """Runs `generate` for every prompt concurrently, returning results in the same order as `prompts`."""
        return list(await asyncio.gather(*(self.generate(prompt, response_model) for prompt in prompts)))
