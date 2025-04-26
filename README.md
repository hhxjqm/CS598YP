# 📘 DuckDB Ingestion Benchmark with Resource Monitoring

This project provides a Docker-based environment to test the **ingestion performance** of DuckDB under resource constraints. It inserts data in batches and logs **CPU usage**, **memory (RSS)**, and **insertion time** for each batch to a CSV file.

---

## ✅ Goals

- 分析 ingestion rate drop 的原因

- 测试 DuckDB 的极限（系统崩不崩、何时崩）

---

## 0. Download test data set
```bash
wget -c -O data_set/taxi_data.json 'https://data.cityofnewyork.us/api/views/4b4i-vvec/rows.json?accessType=DOWNLOAD'
```

```bash
wget -c -O data_set/2023_Yellow_Taxi_Trip_Data.csv 'https://data.cityofnewyork.us/api/views/4b4i-vvec/rows.csv?accessType=DOWNLOAD'
```

## 1. Build the Docker Image
```bash
docker build -t cs598_final .
```

## 2. Run the Container with Resource Limits (4GB of memory and 2 CPU cores)
```bash
docker run --rm -it \
  --memory=4g --memory-swap=4g \
  --cpus=2 \
  -v "$PWD":/test \
  cs598_final
```

## 3. test duckdb
```bash
python insert_duckdb.py
```

## 4. test rocksdb (C++)
```bash
g++ insert_rocksdb.cpp -o insert_test -std=c++17 -I/opt/homebrew/include -L/opt/homebrew/lib -lrocksdb
```
```bash
./insert_test
```

## 5. test sqlite
```bash
python insert_sqlite.py
```

## 6. plot
```bash
python insert_plot.py
```