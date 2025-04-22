import duckdb
import pandas as pd
import random
import time
import json
import argparse
from datetime import datetime
import os
from ingestion_test import get_system_metrics

# --- 查询生成器：基于时间窗口，有问题命中次数太少。。。 ---
def random_time_filter(df):
    # df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    # valid_times = df['tpep_pickup_datetime'].dropna()
    # if valid_times.empty:
    #     return None
    # min_date = valid_times.min().normalize()
    # max_date = valid_times.max().normalize() - pd.Timedelta(days=1)
    # if min_date >= max_date:
    #     return None
    # d1 = min_date + pd.Timedelta(days=random.randint(0, (max_date - min_date).days))
    # d2 = d1 + pd.Timedelta(days=1)
    # return f"SELECT * FROM {{table}} WHERE tpep_pickup_datetime >= '{d1}' AND tpep_pickup_datetime < '{d2}'", "time_window"
    pass


def random_month_filter(df):
    try:
        df['tpep_pickup_datetime'] = pd.to_datetime(
            df['tpep_pickup_datetime'],
            format='%m/%d/%Y %I:%M:%S %p',
            errors='coerce'
        )
    except Exception:
        return None
    valid_times = df['tpep_pickup_datetime'].dropna()
    if valid_times.empty:
        return None
    # 获取所有月初的日期（去重）
    months = valid_times.dt.to_period('M').dropna().unique()
    if len(months) == 0:
        return None
    # 随机选择一个月
    chosen_month = random.choice(months)
    d1 = chosen_month.to_timestamp()
    d2 = (chosen_month + 1).to_timestamp()
    return f"SELECT * FROM {{table}} WHERE tpep_pickup_datetime >= '{d1}' AND tpep_pickup_datetime < '{d2}'", "month_window"

# --- 查询生成器：Top-K 地点 ---
def random_topk_location(df):
    return "SELECT pulocationid, COUNT(*) FROM {table} GROUP BY pulocationid ORDER BY COUNT(*) DESC LIMIT 10", "topk"

# --- 查询生成器：过滤 trip_distance 和 total_amount ---
def random_filter_range(df):
    d_min, d_max = df['trip_distance'].quantile([0.3, 0.9])
    a_min, a_max = df['total_amount'].quantile([0.3, 0.9])
    d = round(random.uniform(d_min, d_max), 2)
    a = round(random.uniform(a_min, a_max), 2)
    return f"SELECT * FROM {{table}} WHERE trip_distance > {d} AND total_amount > {a}", "filter_range"

# --- 查询生成器：随机 group by 某列 ---
def random_groupby(df):
    col = random.choice(['payment_type', 'passenger_count'])
    return f"SELECT {col}, COUNT(*) FROM {{table}} GROUP BY {col}", "groupby"

# --- 调用多个查询生成器，返回随机查询列表 ---
def generate_random_queries(df, table):
    query_generators = [random_month_filter, random_groupby, random_topk_location, random_filter_range]
    queries = []
    for gen in query_generators:
        result = gen(df)
        if not result:
            continue
        sql, qtype = result
        queries.append({
            'sql': sql.format(table=table),
            'type': qtype
        })
    return queries

# --- 执行查询 + 系统状态监控 ---
def run_query(con, sql):
    start = time.time()
    result = con.execute(sql).fetchall()
    end = time.time()
    sys_metrics = get_system_metrics()
    return {
        'row_count': len(result),
        'time_taken_seconds': round(end - start, 5),
        'cpu_percent': sys_metrics.get('cpu_percent', -1),
        'memory_percent': sys_metrics.get('memory_percent', -1),
        'memory_used_gb': sys_metrics.get('memory_used_gb', -1)
    }

# --- 主测试逻辑：循环执行查询，实时写入日志 ---
def benchmark_queries(db_path, table_name, sample_csv, log_path, rounds, max_seconds=None):
    df = pd.read_csv(sample_csv, nrows=10000)
    con = duckdb.connect(db_path)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    start_time = time.time()

    with open(log_path, 'a', encoding='utf-8') as f:
        for i in range(rounds):
            if max_seconds and (time.time() - start_time) > max_seconds:
                print(f"⏱️ 已达到最大运行时间 {max_seconds} 秒，停止测试")
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

# --- CLI 入口 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckDB 查询性能测试（含资源监控）")
    parser.add_argument('--db', required=True, help='DuckDB 数据库路径')
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
