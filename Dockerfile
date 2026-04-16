FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

COPY pyproject.toml README.md ./
COPY harness_analytics ./harness_analytics/
COPY config ./config/

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir ".[web]"

EXPOSE 8000
CMD ["sh", "-c", "uvicorn harness_analytics.server:app --host 0.0.0.0 --port ${PORT:-8000}"]
