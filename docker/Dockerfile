FROM python:3.12-slim

# iproute2 provides `tc` for optional kernel-level netem (docker/netem.sh).
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ddr ./ddr
COPY docker/netem.sh /usr/local/bin/netem.sh

# Bake the deterministic corpus into the image so every container serves
# identical data (same seed -> same bytes) and starts without a build step.
ARG CORPUS_DOCS=500000
RUN python -m ddr.corpus build --docs ${CORPUS_DOCS} --out /app/data/corpus
ENV CORPUS_PATH=/app/data/corpus PYTHONUNBUFFERED=1

EXPOSE 8000
# ROLE=node (default) or ROLE=router
CMD ["sh", "-c", "exec uvicorn ddr.${ROLE:-node}.app:app --host 0.0.0.0 --port 8000 --log-level warning --no-access-log"]
