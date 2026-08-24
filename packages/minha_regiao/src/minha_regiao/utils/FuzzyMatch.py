import unicodedata
from typing import Iterable

import Levenshtein

DEFAULT_THRESHOLD = 0.9


def _strip_accents(text: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char))


def resolve_name(name: str, candidates: Iterable[str], threshold: float = DEFAULT_THRESHOLD) -> str | None:
    """Resolves `name` against `candidates` in three tiers: an exact match,
    then an accent/case-insensitive match (diacritics are the single most
    common source of drift between Portuguese place-name sources and are
    otherwise easy to lose to Levenshtein's ratio on short names), then the
    closest Levenshtein-ratio match above the threshold. Returns None if none
    of the three finds anything.
    """
    candidates = list(candidates)
    if name in candidates:
        return name

    normalized_name = _strip_accents(name).casefold()
    for candidate in candidates:
        if _strip_accents(candidate).casefold() == normalized_name:
            return candidate

    best_candidate, best_ratio = None, 0.0
    for candidate in candidates:
        ratio = Levenshtein.ratio(name, candidate)
        if ratio > best_ratio:
            best_candidate, best_ratio = candidate, ratio

    return best_candidate if best_ratio >= threshold else None
