"""Settings read from the environment, shared by the API and the worker."""
import os
from dataclasses import dataclass, field


def _f(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    redis_url: str = field(default_factory=lambda: os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    lease_seconds: float = field(default_factory=lambda: _f("LEASE_SECONDS", 30))
    base_backoff: float = field(default_factory=lambda: _f("BASE_BACKOFF", 1))
    max_backoff: float = field(default_factory=lambda: _f("MAX_BACKOFF", 60))
    default_max_attempts: int = field(default_factory=lambda: int(_f("DEFAULT_MAX_ATTEMPTS", 4)))
    poll_interval: float = field(default_factory=lambda: _f("POLL_INTERVAL", 0.2))
