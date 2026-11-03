"""API service: accepts jobs (idempotently), reports status and queue stats. Never runs jobs itself."""
from typing import Optional

import redis
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from .config import Settings
from .queue import JobQueue
from .tasks import HANDLERS


class JobRequest(BaseModel):
    type: str
    payload: dict = Field(default_factory=dict)
    idempotency_key: Optional[str] = Field(default=None, max_length=128)
    max_attempts: Optional[int] = Field(default=None, ge=1, le=10)


def create_app(queue: Optional[JobQueue] = None, settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="Task Pipeline API")
    app.state.queue = queue or JobQueue.from_url(settings.redis_url, base_backoff=settings.base_backoff,
                                                 max_backoff=settings.max_backoff)

    def get_queue() -> JobQueue:
        return app.state.queue

    @app.get("/health")
    def health(q: JobQueue = Depends(get_queue)):
        try:
            q.r.ping()
        except redis.RedisError:
            raise HTTPException(status_code=503, detail="redis unavailable")
        return {"status": "ok"}

    @app.post("/jobs", status_code=202)
    def submit(req: JobRequest, q: JobQueue = Depends(get_queue)):
        if req.type not in HANDLERS:
            raise HTTPException(status_code=422, detail=f"unknown job type; choose from {sorted(HANDLERS)}")
        if len(str(req.payload)) > 10_000:
            raise HTTPException(status_code=413, detail="payload too large (10k chars max)")
        job_id, created = q.enqueue(req.type, req.payload, req.idempotency_key,
                                    req.max_attempts or settings.default_max_attempts)
        return {"id": job_id, "status": q.get(job_id)["status"], "deduplicated": not created}

    @app.get("/jobs/{job_id}")
    def status(job_id: str, q: JobQueue = Depends(get_queue)):
        job = q.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.post("/jobs/{job_id}/retry")
    def retry(job_id: str, q: JobQueue = Depends(get_queue)):
        if not q.retry_dead(job_id):
            raise HTTPException(status_code=409, detail="job is not dead-lettered")
        return {"id": job_id, "status": "queued"}

    @app.get("/stats")
    def stats(q: JobQueue = Depends(get_queue)):
        return q.stats()

    return app


app = create_app()  # the Redis client connects lazily, so importing needs no running Redis
