#!/usr/bin/env bash
# =====================================================
# run_mix_rocksdb.sh —— RocksDB 写读混合基准（无副本）
# =====================================================

# ---------- 配置 ----------
CSV="/test/data_set/2023_Yellow_Taxi_Trip_Data.csv"
DB_DIR="/test/db/test_rocksdb/final/test_streamwrite"   # ★ 唯一的 RocksDB 目录
LOG_DIR="/test/log/rocksdb/final"                          # 日志目录
MAX_SEC=3600
DELAY_MIN=0.1
DELAY_MAX=1.0

# ---------- Ctrl-C 时杀掉所有子进程 ----------
trap 'echo "中断，杀掉所有子进程"; kill 0' SIGINT

# ---------- 目录准备 ----------
mkdir -p "$LOG_DIR"  "$(dirname "$DB_DIR")"

# ---------- ingestion：持续写入 ----------
/test/src/rocksdb/build/ingestion_test_streamwrite \
  --csv "$CSV" \
  --db  "$DB_DIR" \
  --log "$LOG_DIR/mixed_streamwrite_${MAX_SEC}s_random.jsonl" \
  --max-seconds "$MAX_SEC" \
  --delay-min  "$DELAY_MIN" \
  --delay-max  "$DELAY_MAX" &
INGEST_PID=$!
echo "ingestion PID: $INGEST_PID"

# ---------- ⏳ 等 RocksDB 写库真正创建完成 ----------
echo -n "等待 RocksDB 初始化..."
while [ ! -f "$DB_DIR/CURRENT" ]; do
  sleep 0.2
done
echo " OK"

# ---------- query：并发只读 ----------
/test/src/rocksdb/build/query_test \
  --csv "$CSV" \
  --db  "$DB_DIR" \
  --log "$LOG_DIR/mixed_query_${MAX_SEC}s.jsonl" \
  --max-seconds "$MAX_SEC" &
QUERY_PID=$!
echo "query PID: $QUERY_PID"

# ---------- 等两主进程结束 ----------
wait "$INGEST_PID" "$QUERY_PID"
echo "✅ 测试完成"
