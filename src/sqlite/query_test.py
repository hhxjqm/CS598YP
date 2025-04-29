import sqlite3
import pandas as pd
import random
import time
import json
import argparse
from datetime import datetime
import os
from src.ingestion_test import get_system_metrics, get_system_metrics_docker
import psutil

# --- 多列 group by ---
def groupby_payment_and_passenger(df):
    return (
        "SELECT payment_type, passenger_count, COUNT(*) FROM {table} GROUP BY payment_type, passenger_count",
        "multi_column_groupby"
    )

# --- top-k 上车地点 ---
def random_topk_location(df):
    return (
        "SELECT pulocationid, COUNT(*) FROM {table} GROUP BY pulocationid ORDER BY COUNT(*) DESC LIMIT 10",
        "aggregation_topk"
    )

# --- 筛选 trip_distance 和 total_amount 范围 ---
def random_filter_range(df):
    d_min, d_max = df['trip_distance'].quantile([0.3, 0.9])
    a_min, a_max = df['total_amount'].quantile([0.3, 0.9])
    d = round(random.uniform(d_min, d_max), 2)
    a = round(random.uniform(a_min, a_max), 2)
    return (
        f"SELECT * FROM {{table}} WHERE trip_distance > {d} AND total_amount > {a}",
        "filter_range"
    )

# --- 随机单列 group by ---
def random_groupby(df):
    col = random.choice(['payment_type', 'passenger_count'])
    return (
        f"SELECT {col}, COUNT(*) FROM {{table}} GROUP BY {col}",
        "single_column_groupby"
    )

# --- 简单 row_number 窗口函数 ---
def window_row_number(df):
    return (
        "SELECT *, ROW_NUMBER() OVER () AS row_num FROM {table}",
        "basic_window"
    )

# --- 按 trip_distance 排序的窗口函数 ---
def sorted_window(df):
    return (
        "SELECT *, ROW_NUMBER() OVER (ORDER BY trip_distance DESC) AS distance_rank FROM {table}",
        "sorted_window"
    )

# --- 全局 quantile 分位数 ---
def quantiles_entire_dataset(df):
    return (
        "SELECT "
        "TOTAL_AMOUNT as total_amount FROM {table}",  # SQLite不支持直接quantile，后面处理
        "quantiles_entire_dataset"
    )

# --- Partition by window function ---
def partition_by_window(df):
    return (
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_type ORDER BY trip_distance DESC) AS rank_within_payment FROM {table}",
        "partition_by_window"
    )

# --- lead lag 分析 ---
def lead_and_lag(df):
    return (
        "SELECT passenger_count, "
        "LEAD(passenger_count) OVER (ORDER BY tpep_pickup_datetime) AS next_passenger, "
        "LAG(passenger_count) OVER (ORDER BY tpep_pickup_datetime) AS prev_passenger "
        "FROM {table}",
        "lead_and_lag"
    )

# --- moving average ---
def moving_averages(df):
    return (
        "SELECT tpep_pickup_datetime, "
        "AVG(total_amount) OVER (ORDER BY tpep_pickup_datetime ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg_amount "
        "FROM {table}",
        "moving_averages"
    )

# --- rolling sum ---
def rolling_sum(df):
    return (
        "SELECT tpep_pickup_datetime, "
        "SUM(total_amount) OVER (ORDER BY tpep_pickup_datetime ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_sum_amount "
        "FROM {table}",
        "rolling_sum"
    )

# --- cumulative sum ---
def range_between(df):
    return (
        "SELECT tpep_pickup_datetime, "
        "SUM(total_amount) OVER (ORDER BY tpep_pickup_datetime ROWS UNBOUNDED PRECEDING) AS cumulative_income "
        "FROM {table}",
        "range_between"
    )

# --- partition quantiles (这里只能模拟) ---
def quantiles_partition_by(df):
    return (
        "SELECT payment_type, AVG(total_amount) as median_amount_within_payment FROM {table} GROUP BY payment_type",
        "quantiles_partition_by"
    )

# --- 复杂多列 group by ---
def multi_column_complex_aggregation(df):
    return (
        "SELECT "
        "passenger_count, payment_type, pulocationid, dolocationid, "
        "strftime('%Y', tpep_pickup_datetime) AS pickup_year, "
        "strftime('%m', tpep_pickup_datetime) AS pickup_month, "
        "COUNT(*) AS trip_count, "
        "SUM(total_amount) AS total_revenue, "
        "AVG(trip_distance) AS avg_distance, "
        "MAX(tip_amount) AS max_tip, "
        "MIN(fare_amount) AS min_fare "
        "FROM {table} "
        "GROUP BY passenger_count, payment_type, pulocationid, dolocationid, pickup_year, pickup_month",
        "multi_column_complex_aggregation"
    )

# --- 生成一批查询 ---
def generate_random_queries(df, table):
    normal_query_generators = [
        random_groupby,
        random_topk_location,
        random_filter_range,
        groupby_payment_and_passenger
    ]

    heavy_query_generators = [
        window_row_number,
        sorted_window,
        quantiles_entire_dataset,
        partition_by_window,
        lead_and_lag,
        moving_averages,
        rolling_sum,
        range_between,
        quantiles_partition_by,
        multi_column_complex_aggregation
    ]

    queries = []
    for i in range(10):
        for gen in normal_query_generators:
            sql, qtype = gen(df)
            queries.append({
                'sql': sql.format(table=table),
                'type': qtype
            })

        heavy_gen = random.choice(heavy_query_generators)
        sql, qtype = heavy_gen(df)
        queries.append({
            'sql': sql.format(table=table),
            'type': qtype
        })

    return queries

# --- 执行查询 ---
def run_query(con, sql):
    p = psutil.Process(os.getpid())
    cpu_times_start = p.cpu_times()
    wall_time_start = time.time()

    try:
        cursor = con.execute(sql)
        result = cursor.fetchall()
    except Exception as e:
        print(f"⚠️ 查询失败: {e}")
        result = []

    wall_time_end = time.time()
    cpu_times_end = p.cpu_times()

    user_diff = cpu_times_end.user - cpu_times_start.user
    system_diff = cpu_times_end.system - cpu_times_start.system
    total_wall_time = wall_time_end - wall_time_start

    cpu_count = psutil.cpu_count(logical=True) or 1

    if total_wall_time > 0:
        raw_cpu_percent = 100 * (user_diff + system_diff) / total_wall_time
        normalized_cpu_percent = raw_cpu_percent / cpu_count
    else:
        normalized_cpu_percent = 0.0

    sys_metrics = get_system_metrics_docker()

    return {
        'row_count': len(result),
        'time_taken_seconds': round(total_wall_time, 5),
        'cpu_percent': round(normalized_cpu_percent, 2),
        'memory_percent': sys_metrics.get('memory_percent', -1),
        'memory_used_gb': sys_metrics.get('memory_used_gb', -1)
    }

def create_indexes_sqlite(con, table_name):
    """
    在SQLite中建推荐索引
    """
    try:
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_passenger_count ON {table_name} (passenger_count);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_payment_type ON {table_name} (payment_type);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_trip_distance ON {table_name} (trip_distance);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_pulocationid ON {table_name} (PULocationID);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_total_amount ON {table_name} (total_amount);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON {table_name} (tpep_pickup_datetime);")
        con.commit()
        print("✅ SQLite索引创建完成")
    except Exception as e:
        print(f"⚠️ 创建SQLite索引失败: {e}")

# --- 主函数：循环执行查询并记录日志 ---
def benchmark_queries(db_path, table_name, sample_csv, log_path, rounds, max_seconds=None):
    df = pd.read_csv(sample_csv, nrows=10000)
    con = sqlite3.connect(db_path)
    create_indexes_sqlite(con, table_name)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    start_time = time.time()

    with open(log_path, 'a', encoding='utf-8') as f:
        for i in range(rounds):
            if max_seconds and (time.time() - start_time) > max_seconds:
                print(f"⏱️ 达到最大运行时间 {max_seconds} 秒，停止测试")
                break

            print(f"\n🔁 第 {i+1} 轮查询")
            for q in generate_random_queries(df, table_name):
                print(f"➡️ 查询类型 [{q['type']}]: {q['sql'][:80]}...")
                result = run_query(con, q['sql'])
                result.update({
                    'timestamp': datetime.now().isoformat(),
                    'query': q['sql'],
                    'query_type': q['type']
                })
                f.write(json.dumps(result) + '\n')
                f.flush()

    print(f"\n✅ 日志写入完成：{log_path}")

# --- CLI入口 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQLite 查询性能测试（含系统监控）")
    parser.add_argument('--db', required=True, help='SQLite 数据库路径')
    parser.add_argument('--table', required=True, help='目标表名')
    parser.add_argument('--sample', required=True, help='样本 CSV 文件路径')
    parser.add_argument('--log', required=True, help='输出日志路径 (.jsonl)')
    parser.add_argument('--rounds', type=int, default=2**31-1, help='最大查询轮数')
    parser.add_argument('--max-seconds', type=int, default=None, help='最大运行时间（秒）')

    args = parser.parse_args()

    benchmark_queries(
        db_path=args.db,
        table_name=args.table,
        sample_csv=args.sample,
        log_path=args.log,
        rounds=args.rounds,
        max_seconds=args.max_seconds
    )
