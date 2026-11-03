"""Job handlers. A handler takes (payload, attempt) and returns a JSON-serializable dict.

Raise PermanentError for failures that retrying cannot fix (bad input); any other exception is retried.
`flaky`, `always_fail` and `slow` exist to demonstrate retries, dead-lettering and leases.
"""
import hashlib
import re
import time
from collections import Counter
from typing import Callable


class PermanentError(Exception):
    """Do not retry: send the job straight to the dead-letter list."""


def checksum(payload: dict, attempt: int) -> dict:
    text = payload.get("text")
    if not isinstance(text, str):
        raise PermanentError("'text' (string) is required")
    return {"sha256": hashlib.sha256(text.encode()).hexdigest(), "length": len(text)}


def wordstats(payload: dict, attempt: int) -> dict:
    text = payload.get("text")
    if not isinstance(text, str):
        raise PermanentError("'text' (string) is required")
    words = re.findall(r"[a-z']+", text.lower())
    return {"words": len(words), "unique": len(set(words)), "top": Counter(words).most_common(5)}


def flaky(payload: dict, attempt: int) -> dict:
    """Fails until it has been attempted `succeed_on_attempt` times (default 3): shows retry + backoff."""
    target = int(payload.get("succeed_on_attempt", 3))
    if attempt < target:
        raise RuntimeError(f"transient failure on attempt {attempt}")
    return {"succeeded_on_attempt": attempt}


def always_fail(payload: dict, attempt: int) -> dict:
    raise RuntimeError("this job always fails (demonstrates the dead-letter list)")


def slow(payload: dict, attempt: int) -> dict:
    """Sleeps `seconds`: used to test lease heartbeats and worker crashes."""
    seconds = float(payload.get("seconds", 1))
    if not 0 <= seconds <= 120:
        raise PermanentError("seconds must be between 0 and 120")
    time.sleep(seconds)
    return {"slept": seconds}


HANDLERS: dict[str, Callable[[dict, int], dict]] = {
    "checksum": checksum, "wordstats": wordstats, "flaky": flaky, "always_fail": always_fail, "slow": slow,
}
