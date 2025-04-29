#!/bin/bash

# Ctrl+C 时杀掉所有子进程（包括同步进程）
trap 'echo "中断，杀掉所有子进程"; kill 0' SIGINT

# 确保最终目录存在
mkdir -p /test/db/duckdb/final

# 定义源库（DB_PATH）和只读副本（DB_READONLY_PATH）
DB_PATH="/test/db/duckdb/final/taxi_data.duckdb"
DB_READONLY_PATH="/test/db/duckdb/final/taxi_data_readonly.duckdb"

# —— 后台启动同步进程（sync process）：等源库文件生成，再首次复制，并每秒更新一次 ——
(
  # 等待源库文件被写入磁盘
  until [ -f "$DB_PATH" ]; do
    echo "⏳ 等待源库文件 (DB_PATH): $DB_PATH"
    sleep 1
  done

  # 第一次复制，确保目标有内容
  cp "$DB_PATH" "$DB_READONLY_PATH"
  echo "✅ 首次复制完成：$DB_READONLY_PATH"

  # 持续循环，每秒同步最新内容
  while true; do
    cp "$DB_PATH" "$DB_READONLY_PATH"
    sleep 1
  done
) &
SYNC_PID=$!
echo "同步进程 PID: $SYNC_PID"

# —— 先启动查询脚本（query script），它内部会轮询等待表存在 ——
python -m src.duckdb.query_test \
  --db "$DB_READONLY_PATH" \
  --table yellow_taxi_trips \
  --sample /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --log /test/log/duckdb/final/mixed_test_1h_log_query.jsonl \
  --max-seconds 3600 &
QUERY_PID=$!
echo "查询进程 PID: $QUERY_PID"

# 等几秒，确保查询脚本已经在执行等待逻辑中了
sleep 5

# —— 然后再启动写入脚本（ingestion script），开始向源库写入数据 ——
python -m src.duckdb.ingestion_test_streamwrite \
  --csv /test/data_set/2023_Yellow_Taxi_Trip_Data.csv \
  --db "$DB_PATH" \
  --table yellow_taxi_trips \
  --log /test/log/duckdb/final/mixed_test_1h_log_ingest.jsonl \
  --max-seconds 3600 \
  --delay-min 0.1 \
  --delay-max 1.0 &
INGEST_PID=$!
echo "写入进程 PID: $INGEST_PID"

# 等待写入和查询两个主进程都结束
wait $INGEST_PID $QUERY_PID

# 停掉同步进程
kill "$SYNC_PID"
echo "测试结束，已停止数据库同步"
