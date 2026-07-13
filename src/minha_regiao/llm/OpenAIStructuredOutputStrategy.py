import logging
from typing import TypeVar

from openai import AsyncOpenAI
from pydantic import BaseModel

from minha_regiao.llm.StructuredOutputStrategy import StructuredOutputStrategy

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class OpenAIStructuredOutputStrategy(StructuredOutputStrategy):
    """Wraps an OpenAI-compatible chat completions API (e.g. OpenRouter) for structured-output
    generation, using the SDK's `.chat.completions.parse()` helper to validate the response
    straight into a Pydantic model.

    No client-side throttling here — unlike GoogleStructuredOutputStrategy, this isn't sitting
    behind a tight free-tier requests-per-minute cap.
    """

    def __init__(self, api_key: str, model: str, base_url: str | None = None, enable_reasoning: bool = False):
        self._client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._enable_reasoning = enable_reasoning

    async def generate(self, prompt: str, response_model: type[T]) -> T:
        logger.info(f"Requesting structured output from {self._model} (schema: {response_model.__name__})")

        completion = await self._client.chat.completions.parse(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            response_format=response_model,
            extra_body={"reasoning": {"enabled": self._enable_reasoning}},
        )

        message = completion.choices[0].message
        
        if message.refusal:
            raise ValueError(f"Model refused to generate structured output: {message.refusal}")

        return message.parsed
