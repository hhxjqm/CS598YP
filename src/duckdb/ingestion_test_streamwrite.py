import random
import time
import duckdb
import os
import pandas as pd
from datetime import datetime
import json
import argparse
from src.ingestion_test import get_system_metrics, get_system_metrics_docker
import psutil

def simulate_random_streaming(csv_file, db_file, table_name, log_file,
                              max_rows=None, max_seconds=None, delay_range=(0.1, 1.0),
                              mode='random'):
    # --- 加载 CSV 样本数据 ---
    all_data = pd.read_csv(csv_file, nrows=5000)  # 避免过大样本引发内存问题
    all_data.columns = all_data.columns.str.lower()
    row_count = len(all_data)
    row_indices = list(range(row_count))

    start_time = time.time()
    total_written = 0

    with duckdb.connect(database=db_file, read_only=False) as con:
        with open(log_file, 'a', encoding='utf-8') as log_f:
            while True:
                # --- 停止条件判断 ---
                if max_rows is not None and total_written >= max_rows:
                    print("✅ 达到最大写入行数限制，停止。")
                    break
                if max_seconds is not None and (time.time() - start_time) >= max_seconds:
                    print("✅ 达到最大运行时间限制，停止。")
                    break

                # --- 写入策略 ---
                if mode == 'fixed_rows':
                    batch_size = 10
                    delay = 1.0
                elif mode == 'scheduled_pattern':
                    minutes_passed = int((time.time() - start_time) // 60)
                    batch_size = (minutes_passed % 12) + 1
                    delay = 1.0
                else:  # 默认 random 模式
                    batch_size = random.randint(1, 100)
                    delay = random.uniform(*delay_range)

                chosen_indices = random.choices(row_indices, k=batch_size)
                batch_df = all_data.iloc[chosen_indices].copy()

                # --- 时间列和数值列格式转换 ---
                for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                    if col in batch_df.columns:
                        batch_df[col] = pd.to_datetime(batch_df[col], errors='coerce')
                for col in ['vendorid', 'passenger_count', 'trip_distance']:
                    if col in batch_df.columns:
                        batch_df[col] = pd.to_numeric(batch_df[col], errors='coerce')

                # --- 插入数据并记录日志 ---
                # --- 写入前，记录开始时间和开始的 CPU 使用时间 ---
                p = psutil.Process(os.getpid())
                cpu_times_start = p.cpu_times()
                wall_time_start = time.time()

                try:
                    if not con.execute(
                            f"SELECT * FROM information_schema.tables WHERE table_name='{table_name}'"
                    ).fetchall():
                        print(f"⚠️ 表 {table_name} 不存在，自动创建中...")
                        duckdb.from_df(all_data.sample(n=10), connection=con).create(table_name)
                        print(f"✅ 表已创建")

                    # --- 写入数据 ---
                    duckdb.from_df(batch_df, connection=con).insert_into(table_name)

                    # --- 写入后，记录结束时间和结束的 CPU 使用时间 ---
                    wall_time_end = time.time()
                    cpu_times_end = p.cpu_times()

                    # --- 计算时间差 ---
                    user_diff = cpu_times_end.user - cpu_times_start.user  # 用户态时间
                    system_diff = cpu_times_end.system - cpu_times_start.system  # 内核态时间
                    total_wall_time = wall_time_end - wall_time_start  # 总耗时（秒）

                    # --- 获取逻辑核数 ---
                    cpu_count = psutil.cpu_count(logical=True) or 1

                    # --- 计算归一化 CPU 使用率 ---
                    if total_wall_time > 0:
                        raw_cpu_percent = 100 * (user_diff + system_diff) / total_wall_time
                        normalized_cpu_percent = raw_cpu_percent / cpu_count
                    else:
                        normalized_cpu_percent = 0.0

                    # --- 获取容器或宿主机系统指标 ---
                    metrics = get_system_metrics_docker()

                    # --- 记录日志 ---
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'status': 'SUCCESS',
                        'rows_ingested': batch_size,
                        'time_taken_seconds': round(total_wall_time, 5),
                        'ingestion_rate_rows_per_sec': round(batch_size / max(total_wall_time, 0.0001), 2),
                        'cpu_percent': round(normalized_cpu_percent, 2),  # ⭐ 新增准确的 CPU 使用率
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

                time.sleep(delay)

# --- CLI 参数解析 ---
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
    parser.add_argument('--mode', type=str, default='random',
                        choices=['random', 'fixed_rows', 'scheduled_pattern'],
                        help='写入模式：random | fixed_rows | scheduled_pattern')

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
        delay_range=(args.delay_min, args.delay_max),
        mode=args.mode
    )
