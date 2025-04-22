import random
import time
import duckdb
import os
import pandas as pd
from datetime import datetime
import json
import argparse
from ingestion_test import get_system_metrics

def simulate_random_streaming(csv_file, db_file, table_name, log_file,
                              max_rows=None, max_seconds=None, delay_range=(0.1, 1.0)):
    all_data = pd.read_csv(csv_file, nrows=5000) # Limit rows to avoid crash
    all_data.columns = all_data.columns.str.lower()

    row_count = len(all_data)
    row_indices = list(range(row_count))

    start_time = time.time()
    total_written = 0
    with duckdb.connect(database=db_file, read_only=False) as con:
        with open(log_file, 'a', encoding='utf-8') as log_f:
            while True:
                # --- 判断是否到达结束条件 ---
                if max_rows is not None and total_written >= max_rows:
                    print("✅ 达到最大写入行数限制，停止。")
                    break
                if max_seconds is not None and (time.time() - start_time) >= max_seconds:
                    print("✅ 达到最大运行时间限制，停止。")
                    break

                # --- 随机选取写入行数 ---
                batch_size = random.randint(1, 100)
                chosen_indices = random.choices(row_indices, k=batch_size)
                batch_df = all_data.iloc[chosen_indices].copy()

                # --- 时间字段解析 ---
                for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                    if col in batch_df.columns:
                        batch_df[col] = pd.to_datetime(batch_df[col], errors='coerce')

                # --- 数值列解析 ---
                for col in ['vendorid', 'passenger_count', 'trip_distance']:
                    if col in batch_df.columns:
                        batch_df[col] = pd.to_numeric(batch_df[col], errors='coerce')

                # --- 插入计时 ---
                start = time.time()
                try:
                    if not con.execute(f"SELECT * FROM information_schema.tables WHERE table_name='{table_name}'").fetchall():
                        print(f"⚠️ 表 {table_name} 不存在，自动创建中...")
                        sample_df = all_data.sample(n=10)
                        duckdb.from_df(sample_df, connection=con).create(table_name)
                        print(f"✅ 表创建成功")
                    duckdb.from_df(batch_df, connection=con).insert_into(table_name)
                    end = time.time()

                    metrics = get_system_metrics()
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'status': 'SUCCESS',
                        'rows_ingested': batch_size,
                        'time_taken_seconds': round(end - start, 5),
                        'ingestion_rate_rows_per_sec': round(batch_size / max(end - start, 0.0001), 2),
                        'system_metrics': {
                            'cpu_percent': metrics['cpu_percent'],
                            'memory_percent': metrics['memory_percent'],
                        }
                    }
                    log_f.write(json.dumps(log_entry) + '\n')
                    log_f.flush()
                    print(f"✅ 写入 {batch_size} 行成功，耗时 {end - start:.4f} 秒")
                    total_written += batch_size

                except Exception as e:
                    print(f"❌ 插入失败: {e}")

                # --- 模拟 sensor 等待 ---
                time.sleep(random.uniform(*delay_range))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="模拟传感器随机写入 DuckDB")
    parser.add_argument('--csv', required=True, help='输入 CSV 文件路径')
    parser.add_argument('--db', required=True, help='DuckDB 数据库文件路径')
    parser.add_argument('--table', required=True, help='目标表名')
    parser.add_argument('--log', required=True, help='日志输出文件路径')
    parser.add_argument('--max-rows', type=int, default=None, help='最多写入行数（可选）')
    parser.add_argument('--max-seconds', type=int, default=None, help='最多运行时间（秒，可选）')
    parser.add_argument('--delay-min', type=float, default=0.1, help='最小写入间隔（秒）')
    parser.add_argument('--delay-max', type=float, default=1.0, help='最大写入间隔（秒）')

    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    os.makedirs(os.path.dirname(args.db), exist_ok=True)

    simulate_random_streaming(
        csv_file=args.csv,
        db_file=args.db,
        table_name=args.table,
        log_file=args.log,
        max_rows=args.max_rows,
        max_seconds=args.max_seconds,
        delay_range=(args.delay_min, args.delay_max)
    )
