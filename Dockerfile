# Dockerfile

FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    duckdb \
    pandas \
    numpy \
    tqdm \
    psutil

WORKDIR /test

COPY monitor_ingestion.py /test/

CMD ["bash"]
