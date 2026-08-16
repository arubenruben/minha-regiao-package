import logging
from typing import TypeVar

import instructor
from openai import AsyncOpenAI
from pydantic import BaseModel

from minha_regiao.llm.StructuredOutputStrategy import StructuredOutputStrategy

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class OpenAIStructuredOutputStrategy(StructuredOutputStrategy):
    """Wraps an OpenAI-compatible chat completions API (e.g. OpenRouter) for structured-output
    generation via instructor: the prompt is sent as a tool call the model must fill in, and
    instructor validates the arguments into the Pydantic model, automatically resending the
    request with the validation error if the model gets it wrong.

    No client-side throttling here — unlike GoogleStructuredOutputStrategy, this isn't sitting
    behind a tight free-tier requests-per-minute cap.
    """

    def __init__(self, api_key: str, model: str, base_url: str | None = None, enable_reasoning: bool = False):
        client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._instructor = instructor.from_openai(client, model=model)
        self._model = model
        self._enable_reasoning = enable_reasoning

    async def generate(self, prompt: str, response_model: type[T]) -> T:
        logger.info(f"Requesting structured output from {self._model} (schema: {response_model.__name__})")

        return await self._instructor.create(
            response_model=response_model,
            messages=[{"role": "user", "content": prompt}],
            extra_body={"reasoning": {"enabled": self._enable_reasoning}},
        )
