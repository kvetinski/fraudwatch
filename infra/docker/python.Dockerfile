# Shared image for scoring and delayed-label workers.
FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
# Required by existing worker instrumentation. Move this to pyproject.toml
# streaming extras once the project package manifest is confirmed.
RUN python -m pip install --no-cache-dir '.[mlops,streaming]' 'prometheus-client>=0.20,<1'
USER 10001:10001
