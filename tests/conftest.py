"""Tests run against a real Redis (set REDIS_URL, default db 15). Each test starts from an empty db."""
import os
import sys
from pathlib import Path

import pytest
import redis

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from taskpipe.queue import JobQueue  # noqa: E402

URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def queue():
    client = redis.Redis.from_url(URL, decode_responses=True)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available (start redis-server or set REDIS_URL)")
    client.flushdb()
    yield JobQueue(client, base_backoff=0.05, max_backoff=0.2)
    client.flushdb()
