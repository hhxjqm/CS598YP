#!/bin/bash

# Ctrl+C 时杀掉所有子进程（包括同步进程）
trap 'echo "中断，杀掉所有子进程"; kill 0' SIGINT

# 确保目录存在
mkdir -p /test/log/test_runs
mkdir -p /test/db/test_duckdb

# 定义数据库路径
DB_PATH="/test/db/test_duckdb/test_streamwrite.duckdb"
DB_READONLY_PATH="/test/db/test_duckdb/test_streamwrite_readonly.duckdb"

# 启动同步副本进程（每 1 秒复制一次，写成后台进程）
while true; do
  cp "$DB_PATH" "$DB_READONLY_PATH"
  sleep 1
done &

# 记录同步进程 PID（可选）
SYNC_PID=$!
echo "同步进程 PID: $SYNC_PID"

# 启动 ingestion 脚本
python /test/src/ingestion_test_streamwrite.py \
  --csv /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --db "$DB_PATH" \
  --table yellow_taxi_test_streamwrite \
  --log /test/log/test_runs/mixed_test_1h_log_ingest.jsonl \
  --max-seconds 3600 \
  --delay-min 0.1 \
  --delay-max 1.0 &

# 启动 query 脚本（使用同步后的副本）
python /test/src/query_test.py \
  --db "$DB_READONLY_PATH" \
  --table yellow_taxi_test_streamwrite \
  --sample /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --log /test/log/test_runs/mixed_test_1h_log_query.jsonl \
  --max-seconds 3600 &

# 等待两个主进程执行完
wait

# 结束同步进程
kill "$SYNC_PID"
echo "测试结束，已停止数据库同步"
