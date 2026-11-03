FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements-worker.txt .
RUN pip install --no-cache-dir -r requirements-worker.txt
COPY taskpipe ./taskpipe
RUN useradd --create-home app
USER app
CMD ["python", "-m", "taskpipe.worker"]
