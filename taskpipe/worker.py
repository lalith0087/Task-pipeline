"""Worker service: pulls jobs, runs handlers, heartbeats the lease, acks/fails, and shuts down gracefully."""
import json
import logging
import signal
import threading
import time
import uuid
from typing import Callable, Optional

from .config import Settings
from .queue import JobQueue, Lease
from .tasks import HANDLERS, PermanentError

log = logging.getLogger("worker")


def log_event(**fields) -> None:
    log.info(json.dumps(fields, default=str))


class Worker:
    def __init__(self, queue: JobQueue, handlers: dict[str, Callable] = HANDLERS, lease_seconds: float = 30,
                 poll_interval: float = 0.2, worker_id: Optional[str] = None):
        self.q, self.handlers = queue, handlers
        self.lease_seconds, self.poll_interval = lease_seconds, poll_interval
        self.id = worker_id or uuid.uuid4().hex[:8]
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        log_event(event="worker_started", worker=self.id)
        while not self._stop.is_set():
            if not self.run_once():
                self._stop.wait(self.poll_interval)
        log_event(event="worker_stopped", worker=self.id)

    def run_once(self) -> bool:
        """Housekeeping, then try to process one job. Returns True if a job was handled."""
        self.q.promote_due()
        self.q.reap_expired()
        lease = self.q.dequeue(self.lease_seconds)
        if lease is None:
            return False
        self._process(lease)
        return True

    def _heartbeat(self, lease: Lease, done: threading.Event) -> None:
        while not done.wait(self.lease_seconds / 3):
            if not self.q.extend_lease(lease, self.lease_seconds):
                log_event(event="lease_lost", worker=self.id, job=lease.id)
                return

    def _process(self, lease: Lease) -> None:
        job_id = lease.id
        job = self.q.get(job_id)
        started = time.time()
        done = threading.Event()
        beat = threading.Thread(target=self._heartbeat, args=(lease, done), daemon=True)
        beat.start()
        try:
            handler = self.handlers.get(job["type"])
            if handler is None:
                raise PermanentError(f"unknown job type '{job['type']}'")
            result = handler(job["payload"], job["attempts"])
            outcome = "succeeded" if self.q.ack(lease, result) else "lease_lost"
        except PermanentError as exc:
            outcome = self.q.fail(lease, str(exc), retryable=False) or "lease_lost"
        except Exception as exc:  # noqa: BLE001 - any handler error is retried
            outcome = self.q.fail(lease, f"{type(exc).__name__}: {exc}") or "lease_lost"
        finally:
            done.set()
        log_event(event="job_done", worker=self.id, job=job_id, type=job["type"], attempt=job["attempts"],
                  outcome=outcome, seconds=round(time.time() - started, 3))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    s = Settings()
    worker = Worker(JobQueue.from_url(s.redis_url, base_backoff=s.base_backoff, max_backoff=s.max_backoff),
                    lease_seconds=s.lease_seconds, poll_interval=s.poll_interval)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: worker.stop())  # finish the current job, then exit
    worker.run()


if __name__ == "__main__":
    main()
