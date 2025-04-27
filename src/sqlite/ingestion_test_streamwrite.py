import random
import time
import sqlite3
import os
import pandas as pd
from datetime import datetime
import json
import argparse
from src.ingestion_test import get_system_metrics, get_system_metrics_docker
import psutil

def simulate_random_streaming_sqlite(csv_file, db_file, table_name, log_file,
                                     max_rows=None, max_seconds=None, delay_range=(0.1, 1.0),
                                     mode='random'):
    # --- 加载 CSV 样本数据 ---
    all_data = pd.read_csv(csv_file, nrows=5000)  # 防止一次读太多，占内存
    all_data.columns = all_data.columns.str.lower()  # 列名统一小写
    row_count = len(all_data)
    row_indices = list(range(row_count))  # 构建索引列表，用来随机抽样

    start_time = time.time()
    total_written = 0

    # --- 连接 SQLite 数据库 ---
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # --- 打开日志文件 ---
    with open(log_file, 'a', encoding='utf-8') as log_f:
        while True:
            # --- 判断是否到达停止条件 ---
            if max_rows is not None and total_written >= max_rows:
                print("✅ 达到最大写入行数限制，停止。")
                break
            if max_seconds is not None and (time.time() - start_time) >= max_seconds:
                print("✅ 达到最大运行时间限制，停止。")
                break

            # --- 决定本轮批量大小和延迟 ---
            if mode == 'fixed_rows':
                batch_size = 10
                delay = 1.0
            elif mode == 'scheduled_pattern':
                minutes_passed = int((time.time() - start_time) // 60)
                batch_size = (minutes_passed % 12) + 1
                delay = 1.0
            else:  # 默认 random
                batch_size = random.randint(1, 100)
                delay = random.uniform(*delay_range)

            # --- 从样本中随机抽取 batch_size 行 ---
            chosen_indices = random.choices(row_indices, k=batch_size)
            batch_df = all_data.iloc[chosen_indices].copy()

            # --- 时间列格式转换 ---
            for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                if col in batch_df.columns:
                    batch_df[col] = pd.to_datetime(batch_df[col], errors='coerce')
            # --- 数值列格式转换 ---
            for col in ['vendorid', 'passenger_count', 'trip_distance']:
                if col in batch_df.columns:
                    batch_df[col] = pd.to_numeric(batch_df[col], errors='coerce')

            # --- 写入数据库前记录当前 CPU 和时间 ---
            p = psutil.Process(os.getpid())
            cpu_times_start = p.cpu_times()
            wall_time_start = time.time()

            try:
                # --- 检查表是否存在，如果不存在就创建 ---
                cursor.execute(f"""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table_name,))
                table_exists = cursor.fetchone()

                if not table_exists:
                    print(f"⚠️ 表 {table_name} 不存在，自动创建中...")
                    # 根据样本前10行推测列名和类型简单建表
                    sample = all_data.sample(n=10)
                    # 简单推断类型：都统一用 TEXT
                    columns_sql = ", ".join([f"'{col}' TEXT" for col in sample.columns])
                    create_table_sql = f"CREATE TABLE {table_name} ({columns_sql});"
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print(f"✅ 表已创建")

                # --- 插入 batch 数据 ---
                placeholders = ", ".join(["?"] * len(batch_df.columns))  # (?, ?, ?, ...)
                insert_sql = f"INSERT INTO {table_name} ({', '.join(batch_df.columns)}) VALUES ({placeholders})"
                cursor.executemany(insert_sql, batch_df.astype(str).values.tolist())
                conn.commit()

                # --- 写入后记录 CPU 和时间 ---
                wall_time_end = time.time()
                cpu_times_end = p.cpu_times()

                # --- 计算 CPU 使用率 ---
                user_diff = cpu_times_end.user - cpu_times_start.user
                system_diff = cpu_times_end.system - cpu_times_start.system
                total_wall_time = wall_time_end - wall_time_start

                cpu_count = psutil.cpu_count(logical=True) or 1
                if total_wall_time > 0:
                    raw_cpu_percent = 100 * (user_diff + system_diff) / total_wall_time
                    normalized_cpu_percent = raw_cpu_percent / cpu_count
                else:
                    normalized_cpu_percent = 0.0

                # --- 获取系统指标 ---
                metrics = get_system_metrics_docker()

                # --- 记录日志 ---
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'status': 'SUCCESS',
                    'rows_ingested': batch_size,
                    'time_taken_seconds': round(total_wall_time, 5),
                    'ingestion_rate_rows_per_sec': round(batch_size / max(total_wall_time, 0.0001), 2),
                    'cpu_percent': round(normalized_cpu_percent, 2),
                    'system_metrics': {
                        'memory_percent': metrics['memory_percent'],
                        'memory_used_gb': metrics['memory_used_gb'],
                        'disk_io_counters': metrics.get('disk_io_counters', None)
                    }
                }
                log_f.write(json.dumps(log_entry) + '\n')
                log_f.flush()
                print(f"✅ 写入 {batch_size} 行成功，耗时 {total_wall_time:.4f} 秒，CPU {normalized_cpu_percent:.2f}%")
                total_written += batch_size

            except Exception as e:
                print(f"❌ 插入失败: {e}")

            # --- 延迟下一次写入 ---
            time.sleep(delay)

    conn.close()

# --- CLI 参数解析 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="模拟传感器随机写入 SQLite")
    parser.add_argument('--csv', required=True, help='输入 CSV 文件路径')
    parser.add_argument('--db', required=True, help='SQLite 数据库文件路径')
    parser.add_argument('--table', required=True, help='目标表名')
    parser.add_argument('--log', required=True, help='日志输出文件路径')
    parser.add_argument('--max-rows', type=int, default=None, help='最多写入行数（可选）')
    parser.add_argument('--max-seconds', type=int, default=None, help='最多运行时间（秒，可选）')
    parser.add_argument('--delay-min', type=float, default=0.1, help='最小写入间隔（秒）')
    parser.add_argument('--delay-max', type=float, default=1.0, help='最大写入间隔（秒）')
    parser.add_argument('--mode', type=str, default='random',
                        choices=['random', 'fixed_rows', 'scheduled_pattern'],
                        help='写入模式：random | fixed_rows | scheduled_pattern')

    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    os.makedirs(os.path.dirname(args.db), exist_ok=True)

    simulate_random_streaming_sqlite(
        csv_file=args.csv,
        db_file=args.db,
        table_name=args.table,
        log_file=args.log,
        max_rows=args.max_rows,
        max_seconds=args.max_seconds,
        delay_range=(args.delay_min, args.delay_max),
        mode=args.mode
    )
