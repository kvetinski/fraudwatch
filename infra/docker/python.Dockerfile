# Role-specific application images share only the locked dependency layer.
ARG PYTHON_IMAGE=python@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df
FROM ${PYTHON_IMAGE} AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    HOME=/tmp \
    MPLCONFIGDIR=/tmp/matplotlib
RUN apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends libgomp1 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY requirements/runtime.lock /tmp/runtime.lock
RUN python -m pip install --disable-pip-version-check \
    --root-user-action=ignore --require-hashes -r /tmp/runtime.lock \
    && rm /tmp/runtime.lock
COPY fraudwatch_ml/ /app/fraudwatch_ml/

FROM runtime AS scoring
ARG FRAUDWATCH_BUILD_REVISION=development
ARG FRAUDWATCH_MODEL_DIGEST=unknown
ARG FRAUDWATCH_MODEL_BUNDLE_DIGEST=unknown
LABEL org.opencontainers.image.title="FraudWatch scoring worker" \
      org.opencontainers.image.revision="${FRAUDWATCH_BUILD_REVISION}" \
      org.opencontainers.image.source="https://github.com/kvetinski/fraudwatch" \
      org.opencontainers.image.model.digest="${FRAUDWATCH_MODEL_DIGEST}" \
      org.opencontainers.image.model.bundle.digest="${FRAUDWATCH_MODEL_BUNDLE_DIGEST}"
COPY services/scoring/worker.py /app/services/scoring/worker.py
COPY artifacts/candidate-model/ /models/candidate/
ENV FRAUDWATCH_MODEL_DIR=/models/candidate
USER 10001:10001
EXPOSE 9101
ENTRYPOINT ["python", "/app/services/scoring/worker.py"]

FROM runtime AS labels
ARG FRAUDWATCH_BUILD_REVISION=development
LABEL org.opencontainers.image.title="FraudWatch label worker" \
      org.opencontainers.image.revision="${FRAUDWATCH_BUILD_REVISION}" \
      org.opencontainers.image.source="https://github.com/kvetinski/fraudwatch"
COPY services/scoring/label_worker.py /app/services/scoring/label_worker.py
USER 10001:10001
EXPOSE 9102
ENTRYPOINT ["python", "/app/services/scoring/label_worker.py"]

FROM runtime AS monitor
ARG FRAUDWATCH_BUILD_REVISION=development
LABEL org.opencontainers.image.title="FraudWatch independent monitor" \
      org.opencontainers.image.revision="${FRAUDWATCH_BUILD_REVISION}" \
      org.opencontainers.image.source="https://github.com/kvetinski/fraudwatch"
COPY services/monitor/monitor.py /app/services/monitor/monitor.py
USER 10001:10001
EXPOSE 9103
ENTRYPOINT ["python", "/app/services/monitor/monitor.py"]

FROM runtime AS model-monitor
ARG FRAUDWATCH_BUILD_REVISION=development
ARG FRAUDWATCH_MODEL_DIGEST=unknown
ARG FRAUDWATCH_MODEL_BUNDLE_DIGEST=unknown
LABEL org.opencontainers.image.title="FraudWatch model monitor" \
      org.opencontainers.image.revision="${FRAUDWATCH_BUILD_REVISION}" \
      org.opencontainers.image.source="https://github.com/kvetinski/fraudwatch" \
      org.opencontainers.image.model.digest="${FRAUDWATCH_MODEL_DIGEST}" \
      org.opencontainers.image.model.bundle.digest="${FRAUDWATCH_MODEL_BUNDLE_DIGEST}"
COPY scripts/model_monitor.py /app/scripts/model_monitor.py
COPY artifacts/candidate-model/ /models/candidate/
USER 10001:10001
ENTRYPOINT ["python", "/app/scripts/model_monitor.py"]

FROM runtime AS operations
ARG FRAUDWATCH_BUILD_REVISION=development
LABEL org.opencontainers.image.title="FraudWatch operations" \
      org.opencontainers.image.revision="${FRAUDWATCH_BUILD_REVISION}" \
      org.opencontainers.image.source="https://github.com/kvetinski/fraudwatch"
COPY scripts/migrate_database.py /app/scripts/migrate_database.py
COPY scripts/quarantine.py /app/scripts/quarantine.py
COPY scripts/terminal_reconciliation.py /app/scripts/terminal_reconciliation.py
COPY infra/migrations/ /app/infra/migrations/
USER 10001:10001
ENTRYPOINT ["python"]
CMD ["--version"]
