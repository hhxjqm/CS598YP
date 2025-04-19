# 📘 DuckDB Ingestion Benchmark with Resource Monitoring

This project provides a Docker-based environment to test the **ingestion performance** of DuckDB under resource constraints. It inserts data in batches and logs **CPU usage**, **memory (RSS)**, and **insertion time** for each batch to a CSV file.

---

## ✅ Goals

- 分析 ingestion rate drop 的原因

- 测试 DuckDB 的极限（系统崩不崩、何时崩）

---

## 1. Build the Docker Image

```bash
docker build -t duckdb-ingest .
```

## 2. Run the Container with Resource Limits (4GB of memory and 2 CPU cores)
```bash
docker run --rm -it \
  --memory=4g --memory-swap=4g \
  --cpus=2 \
  -v "$PWD":/test \
  duckdb-ingest
```

## 3. Run test1.py
```bash
python test1.py
```

## 4. plot
```bash
python plot.py
```
