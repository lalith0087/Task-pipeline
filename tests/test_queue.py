import random
import threading

from taskpipe.queue import backoff_delay


def test_backoff_grows_is_capped_and_jittered():
    lo = [backoff_delay(a, 1, 8, rng=lambda: 0.0) for a in range(1, 7)]
    hi = [backoff_delay(a, 1, 8, rng=lambda: 1.0) for a in range(1, 7)]
    assert lo == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]      # half of the capped exponential
    assert hi == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]      # full capped exponential
    assert all(0 <= backoff_delay(3, 1, 8) <= 4 for _ in range(50))


def test_fifo_and_status_transitions(queue):
    a, _ = queue.enqueue("checksum", {"text": "a"}, now=100)
    b, _ = queue.enqueue("checksum", {"text": "b"}, now=101)
    assert queue.get(a)["status"] == "queued"
    lease_a = queue.dequeue(30, now=102)
    assert lease_a.id == a and lease_a.token == 1                # oldest first; token = attempt number
    job = queue.get(a)
    assert job["status"] == "running" and job["attempts"] == 1
    assert queue.ack(lease_a, {"ok": 1}, now=103) is True
    assert queue.get(a)["status"] == "succeeded" and queue.get(a)["result"] == {"ok": 1}
    assert queue.dequeue(30, now=104).id == b
    assert queue.dequeue(30, now=105) is None                    # empty


def test_idempotency_key_deduplicates(queue):
    first, created1 = queue.enqueue("checksum", {"text": "x"}, idempotency_key="k1")
    second, created2 = queue.enqueue("checksum", {"text": "x"}, idempotency_key="k1")
    assert created1 and not created2 and first == second
    assert queue.stats()["ready"] == 1 and queue.stats()["submitted"] == 1


def test_failure_retries_with_backoff_then_succeeds(queue):
    job_id, _ = queue.enqueue("flaky", {}, max_attempts=3, now=100)
    lease = queue.dequeue(30, now=100)
    assert queue.fail(lease, "boom", now=100, rng=lambda: 0.0) == "retry"
    assert queue.get(job_id)["status"] == "retrying"
    assert queue.dequeue(30, now=100) is None                    # still delayed
    assert queue.promote_due(now=100.0) == 0
    assert queue.promote_due(now=200) == 1                       # backoff elapsed
    assert queue.get(job_id)["status"] == "queued"
    lease = queue.dequeue(30, now=200)
    assert lease.id == job_id and lease.token == 2
    assert queue.get(job_id)["attempts"] == 2
    assert queue.ack(lease, {"done": True}, now=201)


def test_exhausted_attempts_go_to_dead_letter_and_can_be_redriven(queue):
    job_id, _ = queue.enqueue("always_fail", {}, max_attempts=2, now=0)
    lease = queue.dequeue(30, now=0)
    assert queue.fail(lease, "e1", now=0) == "retry"
    queue.promote_due(now=1000)
    lease = queue.dequeue(30, now=1000)
    assert queue.fail(lease, "e2", now=1000) == "dead"          # attempts == max_attempts
    assert queue.get(job_id)["status"] == "dead" and queue.stats()["dead"] == 1
    assert queue.retry_dead(job_id) is True
    assert queue.get(job_id)["status"] == "queued" and queue.get(job_id)["attempts"] == 0
    assert queue.retry_dead(job_id) is False                     # no longer dead


def test_permanent_failure_skips_retries(queue):
    job_id, _ = queue.enqueue("checksum", {}, max_attempts=5, now=0)
    lease = queue.dequeue(30, now=0)
    assert queue.fail(lease, "bad input", retryable=False, now=0) == "dead"


def test_expired_lease_is_redelivered_and_stale_worker_is_fenced_off(queue):
    job_id, _ = queue.enqueue("slow", {}, max_attempts=3, now=0)
    lease_a = queue.dequeue(lease_seconds=10, now=0)             # worker A takes it, then stalls
    assert queue.reap_expired(now=5) == 0                        # lease still valid
    assert queue.reap_expired(now=11) == 1                       # lease expired -> retry scheduled
    assert queue.get(job_id)["status"] == "retrying"
    queue.promote_due(now=1000)
    lease_b = queue.dequeue(30, now=1000)                        # worker B gets the redelivery
    assert lease_b.id == job_id and lease_b.token == 2
    # worker A wakes up late: every call it makes is rejected because its token (1) is stale
    assert queue.ack(lease_a, {"from": "A"}, now=1001) is False
    assert queue.fail(lease_a, "A failed", now=1001) is None
    assert queue.extend_lease(lease_a, 30, now=1001) is False
    assert queue.get(job_id)["status"] == "running"              # B's delivery is untouched
    assert queue.ack(lease_b, {"from": "B"}, now=1002) is True
    assert queue.get(job_id)["result"] == {"from": "B"}
    assert queue.stats()["lease_expired"] == 1


def test_ack_without_lease_is_rejected(queue):
    job_id, _ = queue.enqueue("checksum", {"text": "z"}, now=0)
    lease = queue.dequeue(10, now=0)
    queue.reap_expired(now=11)                                   # lease gone
    assert queue.ack(lease, {"late": 1}, now=12) is False
    assert queue.fail(lease, "late", now=12) is None
    assert queue.get(job_id)["status"] == "retrying"


def test_heartbeat_extends_lease_and_fails_after_loss(queue):
    job_id, _ = queue.enqueue("slow", {}, now=0)
    lease = queue.dequeue(10, now=0)
    assert queue.extend_lease(lease, 10, now=8) is True          # deadline now 18
    assert queue.reap_expired(now=12) == 0                       # not expired thanks to the heartbeat
    assert queue.reap_expired(now=20) == 1
    assert queue.extend_lease(lease, 10, now=21) is False        # lease was lost


def test_competing_consumers_never_receive_the_same_job(queue):
    ids = {queue.enqueue("checksum", {"text": str(i)})[0] for i in range(200)}
    got, lock = [], threading.Lock()

    def consume():
        while (lease := queue.dequeue(60)) is not None:
            with lock:
                got.append(lease.id)

    threads = [threading.Thread(target=consume) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(got) == 200 and set(got) == ids                   # each job exactly once, none lost
