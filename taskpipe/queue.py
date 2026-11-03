"""A reliable job queue on Redis.

Guarantees and mechanisms:
  * at-least-once delivery: a job is moved atomically from `ready` to `processing` and given a
    lease; if the worker dies, the lease expires and the job is redelivered;
  * competing consumers: dequeue is a single atomic Lua script, so each job goes to one worker;
  * retries with exponential backoff and jitter via a `delayed` sorted set;
  * a dead-letter list for jobs that exhaust their attempts or fail permanently;
  * idempotent submission with an idempotency key;
  * fencing: every delivery gets a token (its attempt number); a stale worker whose lease expired and whose
    job was redelivered cannot ack, fail or extend it, because its token no longer matches.
"""
import json
import random
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

import redis

P = "tp:"
READY, PROCESSING, LEASE, DELAYED, DEAD, STATS = (P + n for n in ("ready", "processing", "lease", "delayed", "dead", "stats"))
JOB = P + "job:"

_DEQUEUE = """
local id = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
if not id then return false end
redis.call('ZADD', KEYS[3], ARGV[2], id)
local attempts = redis.call('HINCRBY', ARGV[3] .. id, 'attempts', 1)
redis.call('HSET', ARGV[3] .. id, 'status', 'running', 'updated_at', ARGV[1])
return {id, attempts}
"""

# KEYS: lease, processing, delayed, dead   ARGV: id, outcome(ok|retry|dead), now, retry_at, job_key, text, token
_RELEASE = """
if tostring(redis.call('HGET', ARGV[5], 'attempts')) ~= ARGV[7] then return 0 end
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call('LREM', KEYS[2], 1, ARGV[1])
if ARGV[2] == 'ok' then
  redis.call('HSET', ARGV[5], 'status', 'succeeded', 'result', ARGV[6], 'error', '', 'updated_at', ARGV[3])
elseif ARGV[2] == 'retry' then
  redis.call('HSET', ARGV[5], 'status', 'retrying', 'error', ARGV[6], 'updated_at', ARGV[3])
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
else
  redis.call('HSET', ARGV[5], 'status', 'dead', 'error', ARGV[6], 'updated_at', ARGV[3])
  redis.call('LPUSH', KEYS[4], ARGV[1])
end
return 1
"""

# KEYS: lease   ARGV: id, new_deadline, job_key, token
_EXTEND = """
if tostring(redis.call('HGET', ARGV[3], 'attempts')) ~= ARGV[4] then return 0 end
return redis.call('ZADD', KEYS[1], 'XX', 'CH', ARGV[2], ARGV[1])
"""

# KEYS: delayed, ready   ARGV: now, limit, job_prefix
_PROMOTE = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local n = 0
for _, id in ipairs(ids) do
  if redis.call('ZREM', KEYS[1], id) == 1 then
    redis.call('LPUSH', KEYS[2], id)
    redis.call('HSET', ARGV[3] .. id, 'status', 'queued', 'updated_at', ARGV[1])
    n = n + 1
  end
end
return n
"""


def backoff_delay(attempt: int, base: float, cap: float, rng: Callable[[], float] = random.random) -> float:
    """Exponential backoff with 'equal jitter': half fixed, half random, capped."""
    exp = min(cap, base * (2 ** max(attempt - 1, 0)))
    return exp / 2 + rng() * exp / 2


@dataclass(frozen=True)
class Lease:
    """Proof of a delivery: the job id plus the fencing token (attempt number) it was delivered under."""
    id: str
    token: int


class JobQueue:
    def __init__(self, client: redis.Redis, base_backoff: float = 1.0, max_backoff: float = 60.0,
                 job_ttl: int = 7 * 24 * 3600):
        self.r = client
        self.base_backoff, self.max_backoff, self.job_ttl = base_backoff, max_backoff, job_ttl
        self._dequeue = client.register_script(_DEQUEUE)
        self._release = client.register_script(_RELEASE)
        self._promote = client.register_script(_PROMOTE)
        self._extend = client.register_script(_EXTEND)

    @classmethod
    def from_url(cls, url: str, **kw) -> "JobQueue":
        return cls(redis.Redis.from_url(url, decode_responses=True), **kw)

    # ---- producer side -------------------------------------------------
    def enqueue(self, job_type: str, payload: dict, idempotency_key: Optional[str] = None,
                max_attempts: int = 4, now: Optional[float] = None) -> tuple[str, bool]:
        """Returns (job_id, created). With an idempotency key, repeats return the original job."""
        now = time.time() if now is None else now
        job_id = uuid.uuid4().hex
        pipe = self.r.pipeline()
        pipe.hset(JOB + job_id, mapping={
            "id": job_id, "type": job_type, "payload": json.dumps(payload), "status": "queued",
            "attempts": 0, "max_attempts": max_attempts, "result": "", "error": "",
            "created_at": now, "updated_at": now,
        })
        pipe.expire(JOB + job_id, self.job_ttl)
        pipe.execute()
        if idempotency_key:
            if not self.r.set(P + "idem:" + idempotency_key, job_id, nx=True, ex=24 * 3600):
                self.r.delete(JOB + job_id)
                return self.r.get(P + "idem:" + idempotency_key), False
        self.r.lpush(READY, job_id)
        self.r.hincrby(STATS, "submitted", 1)
        return job_id, True

    def get(self, job_id: str) -> Optional[dict]:
        raw = self.r.hgetall(JOB + job_id)
        if not raw:
            return None
        return {
            **raw,
            "payload": json.loads(raw["payload"]),
            "result": json.loads(raw["result"]) if raw.get("result") else None,
            "attempts": int(raw["attempts"]), "max_attempts": int(raw["max_attempts"]),
            "created_at": float(raw["created_at"]), "updated_at": float(raw["updated_at"]),
        }

    def retry_dead(self, job_id: str, now: Optional[float] = None) -> bool:
        """Re-drive a dead-lettered job with a fresh attempt budget."""
        now = time.time() if now is None else now
        if self.r.hget(JOB + job_id, "status") != "dead" or not self.r.lrem(DEAD, 1, job_id):
            return False
        self.r.hset(JOB + job_id, mapping={"attempts": 0, "status": "queued", "error": "", "updated_at": now})
        self.r.lpush(READY, job_id)
        return True

    # ---- consumer side -------------------------------------------------
    def dequeue(self, lease_seconds: float, now: Optional[float] = None) -> Optional[Lease]:
        now = time.time() if now is None else now
        res = self._dequeue(keys=[READY, PROCESSING, LEASE], args=[now, now + lease_seconds, JOB])
        return Lease(res[0], int(res[1])) if res else None

    def extend_lease(self, lease: Lease, lease_seconds: float, now: Optional[float] = None) -> bool:
        """Heartbeat: push the lease deadline out. False if the lease was already lost."""
        now = time.time() if now is None else now
        return bool(self._extend(keys=[LEASE], args=[lease.id, now + lease_seconds, JOB + lease.id, lease.token]))

    def ack(self, lease: Lease, result: dict, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        job_id = lease.id
        ok = self._release(keys=[LEASE, PROCESSING, DELAYED, DEAD],
                           args=[job_id, "ok", now, 0, JOB + job_id, json.dumps(result), lease.token])
        if ok:
            self.r.hincrby(STATS, "succeeded", 1)
        return bool(ok)

    def fail(self, lease: Lease, error: str, retryable: bool = True, now: Optional[float] = None,
             rng: Callable[[], float] = random.random) -> Optional[str]:
        """Record a failure. Returns 'retry', 'dead', or None if this caller no longer holds the lease."""
        now = time.time() if now is None else now
        job_id = lease.id
        attempts, max_attempts = (int(x) for x in self.r.hmget(JOB + job_id, "attempts", "max_attempts"))
        outcome = "retry" if retryable and attempts < max_attempts else "dead"
        retry_at = now + backoff_delay(attempts, self.base_backoff, self.max_backoff, rng)
        ok = self._release(keys=[LEASE, PROCESSING, DELAYED, DEAD],
                           args=[job_id, outcome, now, retry_at, JOB + job_id, error[:500], lease.token])
        if not ok:
            return None
        self.r.hincrby(STATS, "retried" if outcome == "retry" else "dead", 1)
        return outcome

    def promote_due(self, now: Optional[float] = None, limit: int = 100) -> int:
        now = time.time() if now is None else now
        return int(self._promote(keys=[DELAYED, READY], args=[now, limit, JOB]))

    def reap_expired(self, now: Optional[float] = None, limit: int = 100) -> int:
        """Redeliver jobs whose worker stopped heartbeating (crashed or hung)."""
        now = time.time() if now is None else now
        n = 0
        for job_id in self.r.zrangebyscore(LEASE, "-inf", now, start=0, num=limit):
            attempts = self.r.hget(JOB + job_id, "attempts")
            lease = Lease(job_id, int(attempts or 0))   # the reaper acts on the current delivery
            if self.fail(lease, "lease expired (worker lost)", retryable=True, now=now) is not None:
                self.r.hincrby(STATS, "lease_expired", 1)
                n += 1
        return n

    # ---- observability -------------------------------------------------
    def stats(self) -> dict:
        counters = {k: int(v) for k, v in self.r.hgetall(STATS).items()}
        return {
            "ready": self.r.llen(READY), "delayed": self.r.zcard(DELAYED), "in_flight": self.r.zcard(LEASE),
            "dead": self.r.llen(DEAD),
            **{k: counters.get(k, 0) for k in ("submitted", "succeeded", "retried", "lease_expired")},
            "dead_total": counters.get("dead", 0),
        }
