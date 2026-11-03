"""End-to-end check against a running API (stdlib only): python scripts/smoke.py [base_url]"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def wait_healthy(timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if call("GET", "/health")[0] == 200:
                return
        except OSError:
            pass
        time.sleep(1)
    sys.exit("API never became healthy")


def main():
    wait_healthy()
    ids = {}
    for i in range(10):
        ids[f"checksum-{i}"] = call("POST", "/jobs", {"type": "checksum", "payload": {"text": f"doc {i}"}})[1]["id"]
    ids["flaky"] = call("POST", "/jobs", {"type": "flaky", "payload": {"succeed_on_attempt": 3}})[1]["id"]
    ids["dead"] = call("POST", "/jobs", {"type": "always_fail", "max_attempts": 2})[1]["id"]
    a = call("POST", "/jobs", {"type": "wordstats", "payload": {"text": "a a b"}, "idempotency_key": "smoke-1"})[1]
    b = call("POST", "/jobs", {"type": "wordstats", "payload": {"text": "a a b"}, "idempotency_key": "smoke-1"})[1]
    assert a["id"] == b["id"] and b["deduplicated"], "idempotency key did not deduplicate"
    ids["wordstats"] = a["id"]

    deadline, jobs = time.time() + 90, {}
    while time.time() < deadline:
        jobs = {k: call("GET", f"/jobs/{v}")[1] for k, v in ids.items()}
        if all(j["status"] in ("succeeded", "dead") for j in jobs.values()):
            break
        time.sleep(1)
    bad = {k: j["status"] for k, j in jobs.items() if k != "dead" and j["status"] != "succeeded"}
    assert not bad, f"jobs did not succeed: {bad}"
    assert jobs["flaky"]["attempts"] == 3, jobs["flaky"]
    assert jobs["dead"]["status"] == "dead" and jobs["dead"]["attempts"] == 2, jobs["dead"]
    assert call("POST", "/jobs", {"type": "nope"})[0] == 422
    stats = call("GET", "/stats")[1]
    assert stats["dead"] == 1 and stats["succeeded"] >= 12, stats
    print("smoke test passed:", json.dumps(stats))


if __name__ == "__main__":
    main()
