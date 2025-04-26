import duckdb
import time
import os
import json
from datetime import datetime
import psutil
import pandas as pd

# --- 配置参数 ---
csv_file = 'data_set/2023_Yellow_Taxi_Trip_Data.csv'
db_base_dir = 'db'
log_base_dir = 'log'
chunk_size = 10000

# 测试的 memory_limit 和 threads 参数组合 【新增】
memory_limits = ['256MB', '512MB', '1GB', '4GB']
threads_list = [1, 2, 4]

# --- 工具函数 ---
def get_system_metrics():
    metrics = {}
    try:
        metrics['cpu_percent'] = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        metrics['memory_percent'] = mem.percent
        metrics['memory_used_gb'] = round(mem.used / (1024**3), 2)
        metrics['memory_available_gb'] = round(mem.available / (1024**3), 2)
        metrics['disk_io_counters'] = psutil.disk_io_counters()
    except Exception as e:
        metrics['error'] = str(e)
        metrics['cpu_percent'] = -1
        metrics['memory_percent'] = -1
        metrics['disk_io_counters'] = None
    return metrics

# --- 核心函数 (加了 threads 参数) 【修改】---
def ingest_and_monitor(csv_file, db_file, table_name, log_file, chunk_size, memory_limit, threads):
    total_rows_ingested = 0
    total_time_taken = 0
    chunk_index = 0
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    config = {'memory_limit': memory_limit, 'threads': threads}
    
    try:
        with duckdb.connect(database=db_file, read_only=False, config=config) as con:
            con.execute(f"DROP TABLE IF EXISTS {table_name};")
            con.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{csv_file}') LIMIT 0;
            """)

            with open(log_file, 'a', encoding='utf-8') as log_f:
                csv_iterator = pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False)
                
                for i, chunk_df in enumerate(csv_iterator):
                    chunk_index = i + 1
                    rows_in_chunk = len(chunk_df)
                    if rows_in_chunk == 0:
                        continue

                    chunk_df.columns = chunk_df.columns.str.lower()

                    # 选做：如果有 datetime/numeric 清洗的话可以插入这里

                    start_time = time.time()
                    pre_insert_metrics = get_system_metrics()

                    try:
                        duckdb.from_df(chunk_df, connection=con).insert_into(table_name)
                    except Exception as e:
                        log_entry = {
                            'timestamp': datetime.now().isoformat(),
                            'chunk_index': chunk_index,
                            'status': 'ERROR',
                            'error': str(e),
                            'rows_attempted': rows_in_chunk,
                        }
                        log_f.write(json.dumps(log_entry) + '\n')
                        log_f.flush()
                        continue

                    end_time = time.time()
                    time_taken_chunk = max(end_time - start_time, 0.0001)
                    ingestion_rate_rows_per_sec = rows_in_chunk / time_taken_chunk

                    post_insert_metrics = get_system_metrics()

                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'chunk_index': chunk_index,
                        'status': 'SUCCESS',
                        'rows_ingested': rows_in_chunk,
                        'time_taken_seconds': round(time_taken_chunk, 4),
                        'ingestion_rate_rows_per_sec': round(ingestion_rate_rows_per_sec, 2),
                        'system_metrics_after_chunk': {
                            'cpu_percent': post_insert_metrics.get('cpu_percent', -1),
                            'memory_percent': post_insert_metrics.get('memory_percent', -1),
                            'memory_used_gb': post_insert_metrics.get('memory_used_gb', -1),
                        },
                    }
                    log_f.write(json.dumps(log_entry) + '\n')
                    log_f.flush()

                    total_rows_ingested += rows_in_chunk
                    total_time_taken += time_taken_chunk

    except duckdb.Error as e:
        print(f"DuckDB error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    overall_avg_rate = total_rows_ingested / total_time_taken if total_time_taken > 0 else 0
    print(f"Finished: {table_name} | Memory={memory_limit}, Threads={threads}")
    print(f"Total rows: {total_rows_ingested}, Total time: {total_time_taken:.2f}s, Avg rate: {overall_avg_rate:.2f} rows/sec")
    print("-" * 60)

# --- 主程序 【新增：批量 sweep】---
if __name__ == "__main__":
    for memory_limit in memory_limits:
        for threads in threads_list:
            experiment_id = f"{memory_limit.replace('B','').lower()}_{threads}threads"
            db_file = f"{db_base_dir}/taxi_data_{experiment_id}.duckdb"
            log_file = f"{log_base_dir}/ingestion_log_{experiment_id}.jsonl"
            table_name = 'yellow_taxi_trips'

            print(f"Running experiment: Memory={memory_limit}, Threads={threads}")
            ingest_and_monitor(csv_file, db_file, table_name, log_file, chunk_size, memory_limit, threads)
