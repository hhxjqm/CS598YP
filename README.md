
## Existing evaluations
1. [Benchmarking Ourselves over Time at DuckDB](https://duckdb.org/2024/06/26/benchmarks-over-time.html#appendix)
2. [DuckDB vs SQLite: Performance, Scalability and Features](https://motherduck.com/learn-more/duckdb-vs-sqlite-databases/)
3. [Database-like ops benchmark](https://duckdblabs.github.io/db-benchmark/)

![img.png](./resources/img.png)

## Updated Evaluations Ideas
DuckDB has been extensively tested by its community. The most authoritative existing evaluations are listed above. These works mainly focus on comparing query performance across different databases, DuckDB versions, or system resource overheads. However, one common drawback of these evaluations is that they tend to be too general and lack specialization. For instance, many of their benchmark queries are synthetic or doctrinal, and offer limited semantic meaning. Given that DuckDB is increasingly used in edge computing scenarios, we propose a real-world edge analytics setting based on the NYC Yellow Taxi dataset. Specifically, **the scenario could be on-device analytics within a smart taxi system, where DuckDB is embedded into the vehicles‘ edge unit to process trip data in real time**. **The goal is to evaluate whether DuckDB can handle typical analytical tasks — such as location-based aggregations, payment-type breakdowns, or route-level filtering — efficiently and reliably under such realistic constraints.** We aim to assess **whether DuckDB can truly excel in these daily, high-frequency, resource-constrained query workloads.**

We are planning to: (✅ indicateds completed)

#### 1. Simulate the edge computing environment using Docker. ✅ 

CPU Model, CPU Frequency, CPU Core #, Memory Size, Disk SIze,



#### 2. Insert the **entire 2023 NYC Yellow Taxi dataset** (~10M rows) into DuckDB under the constrained environment. ✅

* Measure:
  - **Ingestion rate (rows/sec)**
  - **CPU / Memory usage**
  - **Disk I/O (optional)**
* Plot performance metrics as time-series charts.
* Identify periodic ingestion slowdowns or throughput fluctuations and correlate them with memory pressure, GC behavior, or internal DuckDB buffering strategy by looking into source code



#### 3. Design and implement a **random query generator** ✅ 

* Build a query workload generator tailored to the **schema and semantics** of the taxi dataset.
  * Each query type represents **a common real-world business scenario** in urban mobility platforms.
    * Query types include:
      * `groupby[payment_type]` – e.g., "Count of payment types"
      * `top_k[pulocationid]` – e.g., "Top __ pickup locations"
      * `filter_range[trip_distance, total_amount]` – e.g., "Long and expensive trips"
      * `random_month_window[pickup_datetime]` – e.g., "Trips from a random month"
      * If needed, we can analyze and add more types.
      
      

#### 4. Continuous streaming queries under limited resources(Await running) ✅

> Since we've previously inserted the entire table into duckdb. We can persistent the entire in-memory database into a local `*.duckdb` file. Then next time we can simply connect the database, and query the **specific table**. (Question: Do we need to seperate the table into multiple partitions e.g. table1, table2... and test DuckDB's performance of randomly querying the multiple tables? Or storing all data in one table and testing single table querying performance would be enough?
>
> We perfer to choose to keep the data in a **single large table** for the following reasons:
>
> - Most real-world edge analytics systems prioritize **schema simplicity and local joins**
> - DuckDB internally supports **efficient indexing and columnar access**, which already optimizes large-table queries
> - We aim to evaluate DuckDB's performance in a realistic setup that mirrors embedded analytics **without manual sharding**

* Simulate a **non-stop background querying workload**:
  - **Query frequency**: 0.5 to 1.0 seconds (configurable delay).
  - **Duration**: Up to 1 hour of continuous querying.
  - **Metrics collected** per query:
  
    - Query latency
    - Result row count
    - CPU usage
    - Memory usage
  
    All metrics are logged in `.jsonl` format for later visualization and analysis.
* Run it for **up to 1 hour continuously** under the constrained hardware configuration.



#### 5. Continously streaming writings(Await running) ✅

To simulate a real-world edge data ingestion scenario — such as a taxi dispatch system, a vehicle-side trip recorder, or an IoT-based traffic monitor — we emulate **continuous writing** into DuckDB with randomly sampled records from the existing dataset. 

Instead of generating synthetic data from scratch, we **reuse real records** already present in the NYC Yellow Taxi dataset. This ensures that:

- All incoming records have valid schema, value distributions, and domain semantics
- The write performance reflects realistic data characteristics (e.g., timestamps, location IDs, outliers)

 The ingestion process is designed to mimic a **streaming sensor or event system**, where:

- Each insertion batch consists of **1–100 random rows**, sampled with replacement
- Insertion intervals are randomized between **0.1 to 1.0 seconds**
- Writes are appended continuously to the same table (`yellow_taxi_streaming`), simulating real-time accumulation
- Each write operation is logged, including:
  - Rows inserted
  - Write latency
  - CPU / memory usage post-insertion

 This helps us answer:

- Can DuckDB sustain frequent edge-level writes without significant memory bloat?
- Does ingestion speed degrade over time under memory pressure?
- How sensitive is write performance to batch size and interval jitter?



#### 6. Hybrid workload: continuous querying and writing (Await Running)



#### 7. **Set up SQLite** (def needed) **and Redis** (if possible). Repeat the above procedures and run the same test workflows on both systems to compare their performance with that of DuckDB.



### 8. Extras

Possibility of concurrent querying and writing on duckdb?


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
## 3. Run ingestion_test.py
```bash
python ingestion_test.py
```

## 4. ingestion_plot.py
```bash
python ingestion_plot.py
```

### 1. Stream Writing
```bash
#单表，随机行数插入，有波动。
python /test/src/ingestion_test_streamwrite.py \
  --csv /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --db /test/db/test_duckdb/test_streamwrite.duckdb \
  --table yellow_taxi_test_streamwrite \
  --log /test/log/test_runs/streamwrite_random_1h_log.jsonl \
  --max-seconds 3600 \
  --delay-min 0.1 \
  --delay-max 1.0

# 生成图表
python /test/src/ingestion_plot_streamwrite.py \
  --log /test/log/test_runs/streamwrite_random_1h_log.jsonl \
  --out /test/plots/streamwrite_random_1h.png \
  --title "StreamWrite Ingestion - 1 Hour Test"
```

```bash
# 每个batch固定行数写入，写入间隔0.5秒
python /test/src/ingestion_test_streamwrite.py \
  --csv /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --db /test/db/test_duckdb/test_streamwrite.duckdb \
  --table yellow_taxi_test_streamwrite \
  --log /test/log/test_runs/streamwrite_fixed_1h_log.jsonl \
  --max-seconds 3600 \
  --mode fixed_rows
```

# Plot the result
python /test/src/ingestion_plot_streamwrite.py \
  --log /test/log/test_runs/streamwrite_fixed_1h_log.jsonl \
  --out /test/plots/streamwrite_fixed_1h.png \
  --title "StreamWrite Ingestion - 1 Hour Test"
```


### 2. Querying DuckDB
```bash
python /test/src/query_test.py \
  --db /test/db/taxi_data.duckdb \
  --table yellow_taxi_trips \
  --sample /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --log /test/log/test_runs/query_1h_log.jsonl \
  --max-seconds 3600
```

```bash
python /test/src/query_plot.py \
  --log /test/log/test_runs/query_1h_log.jsonl \
  --out /test/plots/query_1h_metrics.png \
  --title "DuckDB Query Performance - 1 Hour Testing" \
  --width 15 \
  --height 8
```

## 3. query 和 write 同时执行。
duckdb不能同时读取相同的数据库文件。所以必须对被测数据库做一个readonly备份。
./scripts/mixed_test.sh


python /test/src/query_plot.py \
  --log /test/log/test_runs/mixed_test_1h_log_query.jsonl \
  --out /test/plots/mixed_test_query_1h_metrics.png \
  --title "DuckDB Query Performance - 1 Hour Testing" \
  --width 15 \
  --height 8


## 4. sqlite3 跑相同的测试
```bash
python /test/src/sqlite/ingestion_test_streamwrite.py \
  --csv /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --db /test/db/test_sqlite/test_streamwrite.sqlite3 \
  --table yellow_taxi_test_streamwrite \
  --log /test/log/test_runs/sqlite3/streamwrite_random_1h_log.jsonl \
  --max-seconds 3600 \
  --delay-min 0.1 \
  --delay-max 1.0
```


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
