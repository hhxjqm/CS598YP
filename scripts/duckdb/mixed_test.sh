#!/usr/bin/env bash
# ===========================================================
# scripts/duckdb/mixed_test.sh  —— 修正版
# ===========================================================
trap 'echo "中断，杀掉所有子进程"; kill 0' SIGINT

CSV=/test/data_set/2023_Yellow_Taxi_Trip_Data.csv
DB_PATH=/test/db/test_duckdb/test_streamwrite.duckdb
DB_READONLY=/test/db/test_duckdb/test_streamwrite_readonly.duckdb
LOG_DIR=/test/log/duckdb/6
TABLE=yellow_taxi_test_streamwrite
MAX_SEC=600

mkdir -p "$LOG_DIR" "$(dirname "$DB_PATH")"

# ---------- 1. 启动 ingestion（写库） ----------
python -m src.duckdb.ingestion_test_streamwrite \
  --csv "$CSV" \
  --db  "$DB_PATH" \
  --table "$TABLE" \
  --log "$LOG_DIR/mixed_streamwrite_${MAX_SEC}s_random.jsonl" \
  --max-seconds "$MAX_SEC" \
  --delay-min 0.1 --delay-max 1.0 &
INGEST_PID=$!
echo "ingestion PID: $INGEST_PID"

# ---------- 2. 等数据库文件生成 ----------
echo -n "等待数据库文件生成..."
while [ ! -f "$DB_PATH" ]; do sleep 0.2; done
echo " OK"

# ---------- 3. 等目标表真正创建 ----------
echo -n "等待表 $TABLE 创建..."
until duckdb "$DB_PATH" -c "SELECT 1 FROM information_schema.tables WHERE table_name='$TABLE' LIMIT 1;" \
       >/dev/null 2>&1; do
  sleep 0.2
done
echo " OK"

# ---------- 4. 后台同步副本 ----------
while true; do
  cp "$DB_PATH" "$DB_READONLY"
  sleep 1
done &
SYNC_PID=$!
echo "同步进程 PID: $SYNC_PID"

# ---------- 5. 启动 query（读只读副本） ----------
python -m src.duckdb.query_test \
  --db  "$DB_READONLY" \
  --table "$TABLE" \
  --sample "$CSV" \
  --log "$LOG_DIR/mixed_query_${MAX_SEC}s.jsonl" \
  --max-seconds "$MAX_SEC" &
QUERY_PID=$!
echo "query PID: $QUERY_PID"

# ---------- 6. 等两主进程结束 ----------
wait "$INGEST_PID" "$QUERY_PID"

kill "$SYNC_PID"
echo "测试结束，已停止数据库同步"
