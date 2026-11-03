import pytest
from fastapi.testclient import TestClient

from taskpipe.api import create_app


@pytest.fixture
def client(queue):
    return TestClient(create_app(queue=queue))


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_submit_status_and_dedupe(client):
    body = {"type": "checksum", "payload": {"text": "hi"}, "idempotency_key": "abc"}
    r1 = client.post("/jobs", json=body)
    r2 = client.post("/jobs", json=body)
    assert r1.status_code == 202 and r1.json()["deduplicated"] is False
    assert r2.json()["id"] == r1.json()["id"] and r2.json()["deduplicated"] is True
    job = client.get(f"/jobs/{r1.json()['id']}").json()
    assert job["status"] == "queued" and job["payload"] == {"text": "hi"}


def test_validation_errors(client):
    assert client.post("/jobs", json={"type": "nope"}).status_code == 422
    assert client.post("/jobs", json={"type": "checksum", "max_attempts": 99}).status_code == 422
    assert client.post("/jobs", json={"type": "checksum", "payload": {"text": "x" * 20_000}}).status_code == 413
    assert client.get("/jobs/doesnotexist").status_code == 404


def test_stats_and_retry_endpoint(client, queue):
    jid = client.post("/jobs", json={"type": "always_fail", "max_attempts": 1}).json()["id"]
    assert client.post(f"/jobs/{jid}/retry").status_code == 409  # not dead yet
    queue.fail(queue.dequeue(30), "x")
    assert client.get("/stats").json()["dead"] == 1
    assert client.post(f"/jobs/{jid}/retry").status_code == 200
    assert client.get(f"/jobs/{jid}").json()["status"] == "queued"
