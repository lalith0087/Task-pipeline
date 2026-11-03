# Task Pipeline: two services and a reliable queue

A small distributed job-processing system: an **API service** accepts jobs, a **worker service** (scale it to N
replicas) executes them, and **Redis** is the queue between them. I built it to practice the failure handling that
makes queues hard: retries, crashes, duplicates and shutdown.

```
client --HTTP--> API --enqueue--> [ Redis: ready / delayed / processing+leases / dead ] <--lease-- Worker x N
```

## What it guarantees, and how

| Concern | Mechanism |
|---|---|
| Delivery | **At-least-once.** A job moves atomically from `ready` to `processing` and gets a lease; no ack, no completion |
| Competing consumers | Dequeue is one atomic Lua script, so a job goes to exactly one worker (tested with 8 threads, 200 jobs) |
| Worker crash | The lease expires and any worker's housekeeping redelivers the job (`lease_expired` metric) |
| Long jobs | A heartbeat thread extends the lease while the handler runs |
| Stale workers | **Fencing tokens**: each delivery carries its attempt number; a worker that lost its lease cannot ack, fail or extend the job |
| Transient failures | Retries with exponential backoff and jitter through a delayed sorted set |
| Poison jobs | Exhausted attempts, or a `PermanentError`, go to a **dead-letter list**; `POST /jobs/{id}/retry` re-drives them |
| Duplicate submissions | Optional **idempotency key** (`SET NX`) returns the original job |
| Deployments | Workers handle SIGTERM by finishing the current job, then exiting |
| Observability | `GET /stats` counters, JSON log lines from workers, `GET /health` checks Redis |

Because delivery is at-least-once, **handlers must be idempotent**. Exactly-once processing is not claimed.

## API

```bash
docker compose up --build --scale worker=3
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"type":"checksum","payload":{"text":"hello"},"idempotency_key":"req-1"}'
curl localhost:8000/jobs/<id>       # queued | running | retrying | succeeded | dead
curl localhost:8000/stats
```

Job types: `checksum`, `wordstats`, and three demo types: `flaky` (fails until attempt N), `always_fail`
(dead-letters), `slow` (sleeps; for lease and crash tests).

## Verified behaviour

* **21 tests against a real Redis** (CI provides one as a service): queue semantics, fencing, backoff, dead-lettering,
  competing consumers, worker crash recovery, heartbeats, graceful shutdown and the API.
* **Chaos run, done by hand on one machine with real processes:** I `kill -9`'d a worker mid-job. Its 3-second lease
  expired, a second worker took the job as attempt 2 and finished it (`lease_expired: 1`). Sending SIGTERM to a worker
  mid-job let it finish the job (attempt 1, succeeded) and then exit.
* `scripts/smoke.py` runs an end-to-end check over HTTP. It passed locally with Redis, the API and two workers
  as separate processes, and CI runs it against the Docker Compose stack.

## Honest limitations

* The **Docker/Compose setup was not run on my machine** (no Docker installed there). The CI `compose` job is what
  builds and tests it, so check that job's status.
* **Not deployed to a cloud.** Nothing here has run on AWS/Azure/GCP.
* Single Redis node: no replication or failover. Losing Redis loses the queue, apart from the append-only file.
* Workers poll (default 0.2 s) instead of blocking on the queue, and there are no priorities, rate limits or auth.
* The reaper runs inside each worker's loop, so redelivery needs at least one live worker.

## Run without Docker

```bash
pip install -r requirements-dev.txt
redis-server &                       # or set REDIS_URL
pytest -q
uvicorn taskpipe.api:app &  python -m taskpipe.worker &  python scripts/smoke.py
```

## Layout

`taskpipe/queue.py` (queue + Lua scripts), `worker.py`, `api.py`, `tasks.py`; `api/Dockerfile` and
`worker/Dockerfile` (separate images); `docker-compose.yml`; `tests/`; `scripts/smoke.py`.
