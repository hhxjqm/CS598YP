import duckdb
import time
import os
import json
from datetime import datetime
import psutil
import pandas as pd

# --- Basic parameters ---
csv_file = 'data_set/2023_Yellow_Taxi_Trip_Data.csv'
db_base_dir = 'db'
log_base_dir = 'log'
chunk_size = 10000

# Memory and threads configurations to test
memory_limits = ['256MB', '512MB', '1GB', '4GB']
threads_list = [1, 2, 4]

# --- System resource monitoring ---
def get_system_metrics():
    metrics = {}
    try:
        metrics['cpu_percent'] = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        metrics['memory_percent'] = mem.percent
        metrics['memory_used_gb'] = round(mem.used / (1024**3), 2)
        metrics['memory_available_gb'] = round(mem.available / (1024**3), 2)
    except Exception as e:
        metrics['error'] = str(e)
        metrics['cpu_percent'] = -1
        metrics['memory_percent'] = -1
    return metrics

# --- Ingestion function ---
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

                    # Fix datetime columns
                    datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
                    for col in datetime_cols:
                        if col in chunk_df.columns:
                            chunk_df[col] = pd.to_datetime(
                                chunk_df[col], format='%m/%d/%Y %I:%M:%S %p', errors='coerce'
                            )

                    # Fix numeric columns
                    numeric_cols = [
                        'vendorid', 'passenger_count', 'trip_distance', 'ratecodeid',
                        'pulocationid', 'dolocationid', 'payment_type', 'fare_amount',
                        'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                        'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee'
                    ]
                    for col in numeric_cols:
                        if col in chunk_df.columns:
                            chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')

                    start_time = time.time()
                    pre_metrics = get_system_metrics()

                    try:
                        duckdb.from_df(chunk_df, connection=con).insert_into(table_name)
                    except Exception as e:
                        log_entry = {
                            'timestamp': datetime.now().isoformat(),
                            'chunk_index': chunk_index,
                            'status': 'ERROR',
                            'error': str(e),
                            'rows_attempted': rows_in_chunk
                        }
                        log_f.write(json.dumps(log_entry) + '\n')
                        log_f.flush()
                        continue

                    end_time = time.time()
                    time_taken_chunk = max(end_time - start_time, 0.0001)
                    ingestion_rate_rows_per_sec = rows_in_chunk / time_taken_chunk

                    post_metrics = get_system_metrics()

                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'chunk_index': chunk_index,
                        'status': 'SUCCESS',
                        'rows_ingested': rows_in_chunk,
                        'time_taken_seconds': round(time_taken_chunk, 4),
                        'ingestion_rate_rows_per_sec': round(ingestion_rate_rows_per_sec, 2),
                        'system_metrics_after_chunk': {
                            'cpu_percent': post_metrics.get('cpu_percent', -1),
                            'memory_percent': post_metrics.get('memory_percent', -1),
                            'memory_used_gb': post_metrics.get('memory_used_gb', -1),
                        },
                    }
                    log_f.write(json.dumps(log_entry) + '\n')
                    log_f.flush()

                    total_rows_ingested += rows_in_chunk
                    total_time_taken += time_taken_chunk

    except Exception as e:
        print(f"Unexpected error: {e}")

    overall_avg_rate = total_rows_ingested / total_time_taken if total_time_taken > 0 else 0
    print(f"Finished: {table_name} | Memory={memory_limit}, Threads={threads}")
    print(f"Total rows: {total_rows_ingested}, Total time: {total_time_taken:.2f}s, Avg rate: {overall_avg_rate:.2f} rows/sec")
    print("-" * 60)

# --- Batch run all memory and threads settings ---
if __name__ == "__main__":
    for memory_limit in memory_limits:
        for threads in threads_list:
            experiment_id = f"{memory_limit.replace('B','').lower()}_{threads}threads"
            db_file = f"{db_base_dir}/taxi_data_{experiment_id}.duckdb"
            log_file = f"{log_base_dir}/ingestion_log_{experiment_id}.jsonl"
            table_name = 'yellow_taxi_trips'

            print(f"Running experiment: Memory={memory_limit}, Threads={threads}")
            ingest_and_monitor(csv_file, db_file, table_name, log_file, chunk_size, memory_limit, threads)
