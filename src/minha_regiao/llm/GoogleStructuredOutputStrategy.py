import asyncio
import logging
import time
from collections import deque
from typing import TypeVar

from google import genai
from google.genai import errors, types
from pydantic import BaseModel

from minha_regiao.llm.StructuredOutputStrategy import StructuredOutputStrategy

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class _RateLimiter:
    """Caps calls to at most `max_calls` per rolling `period` seconds, serializing callers so a
    burst of concurrent tasks gets spaced out instead of firing all at once."""

    def __init__(self, max_calls: int, period: float = 60.0):
        self._max_calls = max_calls
        self._period = period
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            self._evict_expired()
            if len(self._timestamps) >= self._max_calls:
                wait = self._period - (time.monotonic() - self._timestamps[0])
                if wait > 0:
                    await asyncio.sleep(wait)
                self._evict_expired()
            self._timestamps.append(time.monotonic())

    def _evict_expired(self) -> None:
        cutoff = time.monotonic() - self._period
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()


class GoogleStructuredOutputStrategy(StructuredOutputStrategy):
    """Wraps the Gemini API for structured-output generation (a prompt in, a validated Pydantic
    model out).

    Reasoning is minimal by default (`thinking_level=MINIMAL`): callers that want more can pass
    a higher level explicitly, up to MEDIUM — HIGH is disallowed to keep latency/cost bounded.

    Requests are throttled client-side to stay under the Gemini free tier's per-model quota
    (15 requests/minute as of writing) rather than paying for a higher tier. Any 429 that still
    slips through is retried using the delay the API suggests.
    """

    _FREE_TIER_REQUESTS_PER_MINUTE = 14  # one below the documented quota of 15, as headroom
    _MAX_RETRIES = 5
    _DEFAULT_RETRY_DELAY_SECONDS = 30.0

    def __init__(
        self,
        api_key: str,
        model: str,
        thinking_level: types.ThinkingLevel = types.ThinkingLevel.MINIMAL,
        requests_per_minute: int = _FREE_TIER_REQUESTS_PER_MINUTE,
    ):
        if thinking_level == types.ThinkingLevel.HIGH:
            raise ValueError("thinking_level=HIGH is not allowed")

        self._client = genai.Client(api_key=api_key)
        self._model = model
        self._thinking_level = thinking_level
        self._rate_limiter = _RateLimiter(requests_per_minute)

    async def generate(self, prompt: str, response_model: type[T]) -> T:
        logger.info(f"Requesting structured output from {self._model} (schema: {response_model.__name__})")

        for attempt in range(1, self._MAX_RETRIES + 1):
            await self._rate_limiter.acquire()
            try:
                response = await self._client.aio.models.generate_content(
                    model=self._model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=response_model,
                        thinking_config=types.ThinkingConfig(thinking_level=self._thinking_level),
                    ),
                )
                return response_model.model_validate_json(response.text)
            except errors.ClientError as error:
                if error.code != 429 or attempt == self._MAX_RETRIES:
                    raise
                delay = self._retry_delay_seconds(error) or self._DEFAULT_RETRY_DELAY_SECONDS
                logger.warning(
                    f"Rate limited by Gemini API, retrying in {delay:.0f}s (attempt {attempt}/{self._MAX_RETRIES})"
                )
                await asyncio.sleep(delay)

        raise AssertionError("unreachable")  # loop always returns or raises

    @staticmethod
    def _retry_delay_seconds(error: errors.ClientError) -> float | None:
        details = (error.details or {}).get("error", {}).get("details", [])
        for detail in details:
            if detail.get("@type", "").endswith("RetryInfo"):
                retry_delay = detail.get("retryDelay", "")
                if retry_delay.endswith("s"):
                    try:
                        return float(retry_delay[:-1])
                    except ValueError:
                        return None
        return None
