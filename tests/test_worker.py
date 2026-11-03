import threading
import time

from taskpipe.tasks import HANDLERS
from taskpipe.worker import Worker


def wait_for(queue, job_id, status, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        if queue.get(job_id)["status"] == status:
            return queue.get(job_id)
        time.sleep(0.05)
    raise AssertionError(f"job never reached {status}: {queue.get(job_id)}")


def start(queue, **kw):
    w = Worker(queue, lease_seconds=kw.pop("lease_seconds", 5), poll_interval=0.02, **kw)
    t = threading.Thread(target=w.run, daemon=True)
    t.start()
    return w, t


def test_worker_runs_jobs_end_to_end(queue):
    jid, _ = queue.enqueue("checksum", {"text": "hello"})
    w, t = start(queue)
    job = wait_for(queue, jid, "succeeded")
    assert job["result"]["length"] == 5 and len(job["result"]["sha256"]) == 64
    w.stop(); t.join(2)
    assert not t.is_alive()                                      # graceful shutdown


def test_flaky_job_succeeds_after_retries(queue):
    jid, _ = queue.enqueue("flaky", {"succeed_on_attempt": 3}, max_attempts=4)
    w, t = start(queue)
    job = wait_for(queue, jid, "succeeded")
    assert job["attempts"] == 3 and job["result"] == {"succeeded_on_attempt": 3}
    assert queue.stats()["retried"] == 2
    w.stop(); t.join(2)


def test_always_failing_job_is_dead_lettered(queue):
    jid, _ = queue.enqueue("always_fail", {}, max_attempts=2)
    w, t = start(queue)
    job = wait_for(queue, jid, "dead")
    assert job["attempts"] == 2 and "always fails" in job["error"]
    w.stop(); t.join(2)


def test_bad_input_and_unknown_type_fail_permanently(queue):
    bad, _ = queue.enqueue("checksum", {"nope": 1}, max_attempts=5)
    unknown, _ = queue.enqueue("does_not_exist", {}, max_attempts=5)
    w, t = start(queue)
    assert wait_for(queue, bad, "dead")["attempts"] == 1         # not retried
    assert "unknown job type" in wait_for(queue, unknown, "dead")["error"]
    w.stop(); t.join(2)


def test_crashed_worker_job_is_recovered_by_another_worker(queue):
    jid, _ = queue.enqueue("checksum", {"text": "recover me"}, max_attempts=3)
    assert queue.dequeue(lease_seconds=0.2).id == jid            # a worker takes it and dies silently
    time.sleep(0.3)
    w, t = start(queue)                                          # a healthy worker's housekeeping reaps it
    job = wait_for(queue, jid, "succeeded")
    assert job["attempts"] == 2
    assert queue.stats()["lease_expired"] == 1
    w.stop(); t.join(2)


def test_heartbeat_keeps_a_long_job_from_being_stolen(queue):
    jid, _ = queue.enqueue("slow", {"seconds": 1.2}, max_attempts=3)
    w1, t1 = start(queue, lease_seconds=0.45)                    # lease shorter than the job; heartbeats renew it
    time.sleep(0.2)
    w2, t2 = start(queue, lease_seconds=0.45)                    # second worker would steal it if the lease lapsed
    job = wait_for(queue, jid, "succeeded", timeout=10)
    assert job["attempts"] == 1 and queue.stats()["lease_expired"] == 0
    w1.stop(); w2.stop(); t1.join(2); t2.join(2)


def test_two_workers_share_load_without_duplicates(queue):
    ids = [queue.enqueue("wordstats", {"text": f"a b c {i}"})[0] for i in range(40)]
    ws = [start(queue) for _ in range(2)]
    for jid in ids:
        wait_for(queue, jid, "succeeded")
    assert queue.stats()["succeeded"] == 40                      # exactly one success per job
    for w, t in ws:
        w.stop(); t.join(2)
