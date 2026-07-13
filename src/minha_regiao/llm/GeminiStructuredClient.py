import asyncio
import logging
from typing import TypeVar

from google import genai
from google.genai import types
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class GeminiStructuredClient:
    """Wraps the Gemini API for structured-output generation (a prompt in, a validated Pydantic
    model out). Reusable across the package — not tied to any one flow.

    Reasoning is off by default (`thinking_budget=0`): callers that want the model to reason
    can pass a positive budget explicitly.
    """

    def __init__(self, api_key: str, model: str, thinking_budget: int = 0):
        self._client = genai.Client(api_key=api_key)
        self._model = model
        self._thinking_budget = thinking_budget

    async def generate(self, prompt: str, response_model: type[T]) -> T:
        logger.info(f"Requesting structured output from {self._model} (schema: {response_model.__name__})")

        response = await self._client.aio.models.generate_content(
            model=self._model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=response_model,
                thinking_config=types.ThinkingConfig(thinking_budget=self._thinking_budget),
            ),
        )

        return response_model.model_validate_json(response.text)

    async def generate_batch(self, prompts: list[str], response_model: type[T]) -> list[T]:
        """Runs `generate` for every prompt concurrently, returning results in the same order as `prompts`."""
        return list(await asyncio.gather(*(self.generate(prompt, response_model) for prompt in prompts)))
