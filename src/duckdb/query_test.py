import duckdb
import pandas as pd
import random
import time
import json
import argparse
from datetime import datetime
import os
from src.ingestion_test import get_system_metrics, get_system_metrics_docker

random.seed(22)

# --- 多列 group by：按 payment_type 和 passenger_count 聚合 ---
def groupby_payment_and_passenger(df):
    """
    按 payment_type 和 passenger_count 两列进行 group by 统计数量
    """
    return (
        "SELECT payment_type, passenger_count, COUNT(*) "
        "FROM {table} GROUP BY payment_type, passenger_count",
        "multi_column_groupby"   # 类型统一改为 multi_column_groupby
    )

# --- 查询 top-k 上车地点 ---
def random_topk_location(df):
    """
    统计 pulocationid 出现次数最多的前10个地点
    """
    return (
        "SELECT pulocationid, COUNT(*) "
        "FROM {table} GROUP BY pulocationid "
        "ORDER BY COUNT(*) DESC LIMIT 10",
        "aggregation_topk"   # 类型统一改为 topk_location
    )

# --- 筛选 trip_distance 和 total_amount 范围 ---
def random_filter_range(df):
    d_min, d_max = df['trip_distance'].quantile([0.3, 0.9])
    a_min, a_max = df['total_amount'].quantile([0.3, 0.9])

    d = round(random.uniform(d_min, d_max), 2)
    a = round(random.uniform(a_min, a_max), 2)

    # ⭐ 关键：把两列都 CAST 成 DOUBLE
    return (
        f"SELECT * FROM {{table}} "
        f"WHERE CAST(trip_distance AS DOUBLE) > {d} "
        f"  AND CAST(total_amount  AS DOUBLE) > {a}",
        "filter_range"
    )

# --- 随机选择一列进行 group by ---
def random_groupby(df):
    """
    在 payment_type 或 passenger_count 两列中随机选择一列做 group by
    """
    col = random.choice(['payment_type', 'passenger_count'])
    return (
        f"SELECT {col}, COUNT(*) FROM {{table}} GROUP BY {col}",
        "single_column_groupby"  # 类型统一改为 single_column_groupby
    )

def window_row_number(df):
    """
    1. Basic Window Function: Row Number
        场景
            给每一条出租车订单打一个唯一行号，便于后续处理。
    """
    return (
        "SELECT *, ROW_NUMBER() OVER () AS row_num FROM {table}",
        "basic_window"
    )

def sorted_window(df):
    return (
        "SELECT *, "
        "ROW_NUMBER() OVER (ORDER BY CAST(trip_distance AS DOUBLE) DESC) AS distance_rank "
        "FROM {table}",
        "sorted_window"
    )

def quantiles_entire_dataset(df):
    """
    3. Quantiles over Entire Dataset
        场景
            计算所有乘客支付总金额的中位数、90%分位数，用来了解消费水平分布。
    """
    return (
        "SELECT "
        "quantile_cont(CAST(total_amount AS DOUBLE), 0.5) OVER () AS median_amount, "
        "quantile_cont(CAST(total_amount AS DOUBLE), 0.9) OVER () AS p90_amount "
        "FROM {table}",
        "quantiles_entire_dataset"
    )

def partition_by_window(df):
    """
    4. Partition by Window Function: Row Number within Payment Type
        场景
            在每种支付方式（例如现金、信用卡）内部对订单排序，分析不同支付方式下的订单特点。
    """
    return (
        "SELECT *, "
        "ROW_NUMBER() OVER (PARTITION BY payment_type "
        "ORDER BY CAST(trip_distance AS DOUBLE) DESC) AS rank_within_payment "
        "FROM {table}",
        "partition_by_window"
    )

def lead_and_lag(df):
    """
    5. Lead and Lag Analysis
        场景
            比较每单前后的乘客数量变化，观察高峰期、低谷期特点。
    """
    return (
        "SELECT passenger_count, "
        "LEAD(passenger_count) OVER (ORDER BY tpep_pickup_datetime) AS next_passenger, "
        "LAG(passenger_count) OVER (ORDER BY tpep_pickup_datetime) AS prev_passenger "
        "FROM {table}",
        "lead_and_lag"
    )

def moving_averages(df):
    """
    6. Moving Average over 3 Rows
        场景
            平滑处理连续3单的支付金额，分析乘客消费变化趋势
    """
    return (
        "SELECT tpep_pickup_datetime, "
        # ⭐ 把 total_amount 显式转 DOUBLE，避免 AVG(VARCHAR)
        "AVG(CAST(total_amount AS DOUBLE)) "
        "     OVER (ORDER BY tpep_pickup_datetime "
        "           ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg_amount "
        "FROM {table}",
        "moving_averages"
    )

def rolling_sum(df):
    """
    7. Rolling Sum over 3 Rows
        场景
            计算连续3单的总支付金额，用来观察收入变化。
    """
    return (
        "SELECT tpep_pickup_datetime, "
        # ⭐ 同理：SUM 里 CAST
        "SUM(CAST(total_amount AS DOUBLE)) "
        "     OVER (ORDER BY tpep_pickup_datetime "
        "           ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_sum_amount "
        "FROM {table}",
        "rolling_sum"
    )

def range_between(df):
    """
    8. Cumulative Sum with Range Between
        场景
            计算从开始到当前单的累计收入，用于绘制司机的工作日总收入曲线。
    """
    return (
        "SELECT tpep_pickup_datetime, "
        # ⭐ CAST，再也不会 sum(VARCHAR)
        "SUM(CAST(total_amount AS DOUBLE)) "
        "     OVER (ORDER BY tpep_pickup_datetime "
        "           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_income "
        "FROM {table}",
        "range_between"
    )

def quantiles_partition_by(df):
    """
    9. Quantiles Partitioned by Payment Type
        场景
            计算每种支付方式下的中位支付金额，比较现金和信用卡乘客的消费习惯差异。
    """
    return (
        "SELECT payment_type, "
        # ⭐ quantile_cont 也要数值列
        "quantile_cont(CAST(total_amount AS DOUBLE), 0.5) "
        "     OVER (PARTITION BY payment_type) AS median_amount_within_payment "
        "FROM {table}",
        "quantiles_partition_by"
    )

def multi_column_complex_aggregation(df):
    """
    🔥 复杂多列聚合查询
    场景：
        大量分组维度下，做多种复杂聚合，专门用于打爆CPU和内存
    """
    return (
        "SELECT "
        "passenger_count, "
        "payment_type, "
        "PULocationID, "
        "DOLocationID, "
        "EXTRACT(year  FROM tpep_pickup_datetime)  AS pickup_year, "
        "EXTRACT(month FROM tpep_pickup_datetime)  AS pickup_month, "
        "COUNT(*)                                    AS trip_count, "
        "SUM(CAST(total_amount  AS DOUBLE))          AS total_revenue, "
        "AVG(CAST(trip_distance AS DOUBLE))          AS avg_distance, "
        "MAX(CAST(tip_amount    AS DOUBLE))          AS max_tip, "
        "MIN(CAST(fare_amount   AS DOUBLE))          AS min_fare "
        "FROM {table} "
        "GROUP BY passenger_count, payment_type, PULocationID, "
        "         DOLocationID, pickup_year, pickup_month",
        "multi_column_complex_aggregation"
    )

def random_point_lookup(df):
    """
    模拟点查：随机选择一个 PULocationID 查找对应记录
    """
    loc_id = random.choice(df['PULocationID'].dropna().unique().tolist())
    return (
        f"SELECT * FROM {{table}} WHERE PULocationID = {loc_id} LIMIT 5",
        "point_lookup"
    )

def random_datetime_range(df):
    """
    随机选择一个时间范围，模拟用户查某一小时的记录
    """
    times = pd.to_datetime(df['tpep_pickup_datetime'].dropna(), errors='coerce')
    times = times.dropna()
    if times.empty:
        return (
            f"SELECT * FROM {{table}} WHERE passenger_count = 1 LIMIT 10",
            "fallback_datetime_range"
        )
    start = random.choice(times)
    end = start + pd.Timedelta(hours=1)
    return (
        f"SELECT VendorID, trip_distance, total_amount "
        f"FROM {{table}} "
        f"WHERE CAST(tpep_pickup_datetime AS TIMESTAMP) BETWEEN '{start}' AND '{end}' "
        f"ORDER BY total_amount DESC LIMIT 10",
        "datetime_range"
    )


def random_multi_column_filter(df):
    """
    多列联合过滤：查特定上下车点和乘客数
    """
    loc_id = random.choice(df['PULocationID'].dropna().unique().tolist())
    doloc_id = random.choice(df['DOLocationID'].dropna().unique().tolist())
    pax = random.choice(df['passenger_count'].dropna().unique().tolist())
    return (
        f"SELECT trip_distance, fare_amount, tip_amount "
        f"FROM {{table}} "
        f"WHERE PULocationID = {loc_id} AND DOLocationID = {doloc_id} "
        f"AND passenger_count = {pax} LIMIT 10",
        "multi_column_filter"
    )

def tip_amount_above_zero(df):
    """
    分析有小费的订单：按 VendorID 分组计算平均 tip
    """
    return (
        f"SELECT VendorID, AVG(tip_amount) "
        f"FROM {{table}} WHERE tip_amount > 0 GROUP BY VendorID",
        "nonzero_tip_groupby"
    )



def generate_random_queries(df, table):
    normal_query_generators = [
        random_groupby,
        random_topk_location,
        random_filter_range,
        groupby_payment_and_passenger,

        # 加入新的 realistic 查询函数，只在mix时候运行，独自运行query的时候记得注释。
        random_point_lookup,
        random_datetime_range,
        random_multi_column_filter,
        tip_amount_above_zero
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
    for i in range(10):  # 每10轮
        # 先生成普通查询
        for gen in random.sample(normal_query_generators, k=min(5, len(normal_query_generators))):
            try:
                sql, qtype = gen(df)
                queries.append({
                    'sql': sql.format(table=table),
                    'type': qtype
                })
            except Exception as e:
                print(f"⚠️ 忽略生成查询失败: {gen.__name__} -> {e}")

        # 每轮添加一个 heavy 查询
        heavy_gen = random.choice(heavy_query_generators)
        try:
            sql, qtype = heavy_gen(df)
            queries.append({
                'sql': sql.format(table=table),
                'type': qtype
            })
        except Exception as e:
            print(f"⚠️ 忽略 heavy 查询生成失败: {heavy_gen.__name__} -> {e}")

    return queries


def create_indexes_duckdb(con, table_name):
    """
    在DuckDB中建推荐索引
    """
    try:
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_passenger_count ON {table_name} (passenger_count);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_payment_type ON {table_name} (payment_type);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_trip_distance ON {table_name} (trip_distance);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_pulocationid ON {table_name} (pulocationid);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_total_amount ON {table_name} (total_amount);")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON {table_name} (tpep_pickup_datetime);")
        print("✅ DuckDB索引创建完成")
    except Exception as e:
        print(f"⚠️ 创建DuckDB索引失败: {e}")


# --- 执行查询 + 系统状态监控 ---
import psutil
import os

def run_query(con, sql):
    p = psutil.Process(os.getpid())
    cpu_times_start = p.cpu_times()
    wall_time_start = time.time()

    result = con.execute(sql).fetchall()

    wall_time_end = time.time()
    cpu_times_end = p.cpu_times()

    user_diff = cpu_times_end.user - cpu_times_start.user
    system_diff = cpu_times_end.system - cpu_times_start.system
    total_wall_time = wall_time_end - wall_time_start

    # 获取逻辑CPU核数
    cpu_count = psutil.cpu_count(logical=True) or 1

    if total_wall_time > 0:
        raw_cpu_percent = 100 * (user_diff + system_diff) / total_wall_time
        normalized_cpu_percent = raw_cpu_percent / cpu_count  # ⭐ 除以核数！
    else:
        normalized_cpu_percent = 0.0

    sys_metrics = get_system_metrics_docker()

    return {
        'row_count': len(result),
        'time_taken_seconds': round(total_wall_time, 5),
        'cpu_percent': round(normalized_cpu_percent, 2),   # ⭐ 注意这里
        'memory_percent': sys_metrics.get('memory_percent', -1),
        'memory_used_gb': sys_metrics.get('memory_used_gb', -1)
    }



def wait_for_table(con, table, timeout=30):
    import time
    for _ in range(timeout):
        try:
            # 如果表存在，就直接返回
            con.execute(f"SELECT 1 FROM {table} LIMIT 1")
            print(f"✅ 表 {table} 已准备好")
            return
        except duckdb.CatalogException:
            print(f"⏳ 等待表 {table} 创建中...")
            time.sleep(1)
    raise TimeoutError(f"❌ 表 {table} 在 {timeout} 秒内未出现")

# --- 主测试逻辑：循环执行查询，实时写入日志 ---
def benchmark_queries(db_path, table_name, sample_csv, log_path, rounds, max_seconds=None):
    df = pd.read_csv(sample_csv, nrows=10000)
    print("Successfully loaded")
    con = duckdb.connect(db_path)
    #create_indexes_duckdb(con, table_name)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    wait_for_table(con, table_name, timeout=60)
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
