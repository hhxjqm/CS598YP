# 📘 DuckDB Ingestion Benchmark with Resource Monitoring

This project provides a Docker-based environment to test the **ingestion performance** of DuckDB under resource constraints. It inserts data in batches and logs **CPU usage**, **memory (RSS)**, and **insertion time** for each batch to a CSV file.

---

## ✅ Features

- DuckDB embedded database
- Automatic logging of:
  - Batch number
  - Inserted rows
  - Time taken per batch
  - Memory usage (RSS)
  - CPU utilization
- Results written to `ingestion_log.csv`

---

## 📦 1. Build the Docker Image

```bash
docker build -t duckdb-ingest .
```

```bash
docker run --rm -it \
  --memory=4g --memory-swap=4g \
  --cpus=2 \
  -v "$PWD":/test \
  duckdb-ingest
```