import asyncio
import json
import logging
import time
from collections import deque
from typing import Any, TypeVar

import instructor
from google import genai
from google.genai import errors, types
from instructor.core import AsyncValidationError, InstructorRetryException, ResponseParsingError
from pydantic import BaseModel, ValidationError
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt, wait_exponential

from minha_regiao.llm.StructuredOutputStrategy import StructuredOutputStrategy

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Mirrors instructor's own default retry set (pydantic validation / JSON parsing failures),
# so wiring a custom AsyncRetrying (to also cover 429s below) doesn't drop that behavior.
_RETRYABLE_PARSE_ERRORS = (ValidationError, json.JSONDecodeError, AsyncValidationError, ResponseParsingError)


class _RateLimiter:
    """Caps calls to at most `max_calls` per rolling `period` seconds, serializing callers so a
    burst of concurrent tasks gets spaced out instead of firing all at once."""

    def __init__(self, max_calls: int, period: float):
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


class DailyQuotaExceeded(RuntimeError):
    """Raised when a call would exceed the Gemini free-tier daily request quota."""


class _DailyQuotaGuard:
    """Rejects calls once `max_calls` have been made within a rolling `period` seconds, instead
    of blocking like `_RateLimiter` does — a day-long wait is not something a flow run should
    sit on, so this fails fast so the caller can decide what to do (retry tomorrow, alert, ...).
    """

    def __init__(self, max_calls: int, period: float = 86400.0):
        self._max_calls = max_calls
        self._period = period
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            cutoff = time.monotonic() - self._period
            while self._timestamps and self._timestamps[0] <= cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max_calls:
                raise DailyQuotaExceeded(f"Gemini free-tier daily quota of {self._max_calls} requests reached")
            self._timestamps.append(time.monotonic())


class GoogleStructuredOutputStrategy(StructuredOutputStrategy):
    """Wraps the Gemini API for structured-output generation (a prompt in, a validated Pydantic
    model out) via instructor, so schema validation failures are automatically resent to the
    model for correction instead of surfacing as a hard error.

    Reasoning is minimal by default (`thinking_level=MINIMAL`): callers that want more can pass
    a higher level explicitly, up to MEDIUM — HIGH is disallowed to keep latency/cost bounded.

    Requests are throttled client-side to stay under the Gemini free tier's quota for
    gemini-2.5-flash (5 requests/minute, 20 requests/day, as of writing:
    https://ai.google.dev/gemini-api/docs/rate-limits) rather than paying for a higher tier:
    - the per-minute cap blocks/spaces out calls, since a short wait is cheap;
    - the per-day cap raises `DailyQuotaExceeded` instead of blocking, since a call that would
      otherwise wait out the rest of the day isn't something a flow run should sit on.

    Retries (429s from a quota slipping through, and instructor's own validation reasks) are
    delegated to instructor's tenacity-based retry loop rather than a hand-rolled one, honoring
    the delay the API suggests for 429s.
    """

    _FREE_TIER_REQUESTS_PER_MINUTE = 4  # one below the documented quota of 5, as headroom
    _FREE_TIER_REQUESTS_PER_DAY = 19  # one below the documented quota of 20, as headroom
    _MAX_RETRIES = 5
    _DEFAULT_RETRY_DELAY_SECONDS = 30.0

    def __init__(
        self,
        api_key: str,
        model: str,
        thinking_level: types.ThinkingLevel = types.ThinkingLevel.MINIMAL,
        requests_per_minute: int = _FREE_TIER_REQUESTS_PER_MINUTE,
        requests_per_day: int = _FREE_TIER_REQUESTS_PER_DAY,
    ):
        if thinking_level == types.ThinkingLevel.HIGH:
            raise ValueError("thinking_level=HIGH is not allowed")

        self._model = model
        self._thinking_level = thinking_level

        client = genai.Client(api_key=api_key)
        self._minute_limiter = _RateLimiter(requests_per_minute, period=60.0)
        self._daily_quota = _DailyQuotaGuard(requests_per_day)
        client.aio.models.generate_content = self._throttled(client.aio.models.generate_content)

        self._instructor = instructor.from_genai(client, mode=instructor.Mode.JSON, use_async=True, model=model)

    def _throttled(self, generate_content: Any) -> Any:
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            await self._daily_quota.acquire()
            await self._minute_limiter.acquire()
            return await generate_content(*args, **kwargs)

        return wrapped

    async def generate(self, prompt: str, response_model: type[T]) -> T:
        logger.info(f"Requesting structured output from {self._model} (schema: {response_model.__name__})")

        retrying = AsyncRetrying(
            stop=stop_after_attempt(self._MAX_RETRIES),
            wait=self._retry_wait,
            retry=retry_if_exception(self._is_retryable),
            before_sleep=self._log_retry,
            reraise=True,
        )

        try:
            return await self._instructor.create(
                response_model=response_model,
                messages=[{"role": "user", "content": prompt}],
                thinking_config=types.ThinkingConfig(thinking_level=self._thinking_level),
                max_retries=retrying,
            )
        except InstructorRetryException as exc:
            # Instructor wraps every exhausted-retry failure in its own exception type; unwrap
            # DailyQuotaExceeded so callers can catch it directly instead of an opaque wrapper.
            if isinstance(exc.__cause__, DailyQuotaExceeded):
                raise exc.__cause__ from exc
            raise

    def _is_retryable(self, exception: BaseException) -> bool:
        if isinstance(exception, errors.ClientError):
            return exception.code == 429
        return isinstance(exception, _RETRYABLE_PARSE_ERRORS)

    def _retry_wait(self, retry_state: RetryCallState) -> float:
        exception = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exception, errors.ClientError) and exception.code == 429:
            return self._retry_delay_seconds(exception) or self._DEFAULT_RETRY_DELAY_SECONDS
        return wait_exponential(multiplier=1, max=30)(retry_state)

    def _log_retry(self, retry_state: RetryCallState) -> None:
        exception = retry_state.outcome.exception() if retry_state.outcome else None
        reason = "rate limited by Gemini API" if isinstance(exception, errors.ClientError) else "validation failed"
        logger.warning(
            f"Retrying structured output ({reason}), attempt {retry_state.attempt_number}/{self._MAX_RETRIES}"
        )

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
