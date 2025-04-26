import sqlite3
import time
import os
import json
from datetime import datetime
import psutil
import pandas as pd # 使用 pandas 来分块读取 CSV
import numpy as np # 用于处理 NaN 值

# --- 配置参数 ---
# CSV 数据文件路径 (使用 Google Drive 挂载路径)
csv_file = 'data_set/2023_Yellow_Taxi_Trip_Data.csv'
# 日志文件路径 (使用 Google Drive 挂载路径)
log_file = 'log/ingestion_log_sqlite.jsonl' # 使用 .jsonl 格式，每行一个 JSON 对象
# SQLite 数据库文件路径 (使用 Google Drive 挂载路径)
db_file = 'db/taxi_data.sqlite' # 修改数据库文件名为更具描述性的名字和 .sqlite 扩展名
# 目标表名
table_name = 'yellow_taxi_trips_sqlite' # 为 SQLite 表使用不同的名称
# CSV 读取的块大小 (行数) - 影响每次插入的数据量和监控的粒度
chunk_size = 10000 # 可以根据内存和CPU调整，太小开销大，太大监控不精细

# 系统信息采样间隔 (每次块插入后记录)
# psutil.cpu_percent(interval=None) 是非阻塞的，适合在每次块插入后快速获取
# 如果需要更细粒度的系统采样，可以在单独的线程中进行，但这里简化为每次块插入后采样

# --- 确保目录存在 ---
log_dir = os.path.dirname(log_file)
db_dir = os.path.dirname(db_file)
os.makedirs(log_dir, exist_ok=True)
os.makedirs(db_dir, exist_ok=True)
print(f"确保日志目录存在: {log_dir}")
print(f"确保数据库目录存在: {db_dir}")


# --- 获取系统资源信息的函数 ---
def get_system_metrics():
    """获取当前的系统资源使用情况，不计算delta，只获取当前值"""
    metrics = {}
    try:
        # CPU 使用率 (瞬时)
        metrics['cpu_percent'] = psutil.cpu_percent(interval=None)

        # 内存使用率
        mem = psutil.virtual_memory()
        metrics['memory_percent'] = mem.percent
        metrics['memory_used_gb'] = round(mem.used / (1024**3), 2)
        metrics['memory_available_gb'] = round(mem.available / (1024**3), 2)

        # 磁盘 I/O 计数器
        metrics['disk_io_counters'] = psutil.disk_io_counters()

    except Exception as e:
        print(f"获取系统指标时发生错误: {e}")
        # 返回部分或空指标，避免程序中断
        metrics['error'] = str(e)
        if 'cpu_percent' not in metrics: metrics['cpu_percent'] = -1
        if 'memory_percent' not in metrics: metrics['memory_percent'] = -1
        metrics['disk_io_counters'] = None # 如果获取失败，设置为 None

    return metrics

# --- 主插入和监控函数 (SQLite 版本) ---
def ingest_and_monitor_sqlite(csv_file, db_file, table_name, log_file, chunk_size):
    total_rows_ingested = 0
    total_time_taken = 0
    chunk_index = 0

    print(f"开始从 {csv_file} 插入数据到 SQLite 数据库 {db_file} 的表 {table_name}")
    print(f"日志将记录到 {log_file}")
    print(f"块大小 (chunk size): {chunk_size} 行")

    # 使用 with 语句确保连接和文件关闭
    try:
        # 连接到 SQLite 数据库 (如果不存在则创建)
        with sqlite3.connect(db_file) as conn:
            print("成功连接到 SQLite 数据库。")
            cursor = conn.cursor()

            # 准备表：如果表已存在，删除它以便重新开始
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                print(f"如果存在，已删除旧表 {table_name}。")
            except sqlite3.Error as e:
                 print(f"删除旧表时发生 SQLite 错误: {e}")
            except Exception as e:
                 print(f"删除旧表时发生未预期的错误: {e}")


            # --- 推断表结构并创建表 ---
            try:
                # 使用 pandas 读取 CSV 头部来推断列名和类型
                # low_memory=False helps with mixed types
                temp_df = pd.read_csv(csv_file, nrows=10, low_memory=False)
                # Convert column names to lowercase for consistency
                temp_df.columns = temp_df.columns.str.lower()

                # Mapping pandas dtypes to SQLite types
                # This is a simplified mapping, you might need to adjust based on your data
                dtype_mapping = {
                    'int64': 'INTEGER',
                    'float64': 'REAL',
                    'object': 'TEXT', # Strings, mixed types often become object
                    'bool': 'INTEGER', # SQLite uses 0/1 for boolean
                    'datetime64[ns]': 'TEXT', # SQLite doesn't have a native datetime type, store as TEXT (ISO8601) or INTEGER (Unix epoch)
                                              # Storing as TEXT (ISO8601) is generally more readable
                }

                columns_sql = []
                for col_name, dtype in temp_df.dtypes.items():
                    # Handle potential pandas object type becoming more specific after reading more data
                    # For simplicity, we'll use the initial guess, but be aware of potential type issues
                    sqlite_type = dtype_mapping.get(str(dtype), 'TEXT') # Default to TEXT if type not in mapping
                    columns_sql.append(f'"{col_name}" {sqlite_type}') # Quote column names to handle spaces/special chars

                create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns_sql)});"
                print(f"根据 CSV 结构生成的 CREATE TABLE 语句:\n{create_table_sql}")

                cursor.execute(create_table_sql)
                print(f"创建了新表 {table_name}。")

            except FileNotFoundError:
                print(f"错误: CSV 文件未找到在 {csv_file}")
                return # 如果文件未找到，无法继续
            except Exception as e:
                 print(f"推断表结构或创建表时发生错误: {e}")
                 # You might want to inspect the first few rows of the CSV if this fails
                 # print(temp_df.head().to_markdown()) # Uncomment for debugging
                 return # 如果创建表失败，无法继续


            # 打开日志文件, 'a' mode for appending
            with open(log_file, 'a', encoding='utf-8') as log_f:

                # 使用 pandas 分块读取 CSV
                try:
                    # Read the full CSV in chunks using pandas
                    # low_memory=False can help with mixed types but uses more memory
                    csv_iterator = pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False)
                    print("成功创建 CSV 读取迭代器。")

                    # 获取初始磁盘 I/O 计数器
                    initial_metrics = get_system_metrics()
                    prev_disk_io_counters = initial_metrics.get('disk_io_counters', None)

                    # Prepare INSERT statement template
                    # Use ? as placeholders for values
                    placeholders = ', '.join(['?'] * len(temp_df.columns))
                    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders});"
                    print(f"准备好的 INSERT 语句模板: {insert_sql}")


                    # 迭代处理每个数据块
                    print("开始处理数据块...")
                    for i, chunk_df in enumerate(csv_iterator):
                        chunk_index = i + 1
                        rows_in_chunk = len(chunk_df)

                        if rows_in_chunk == 0:
                            print(f"块 {chunk_index} 为空，跳过。")
                            continue

                        # Convert pandas column names to lowercase for consistency with SQLite
                        chunk_df.columns = chunk_df.columns.str.lower()

                        # --- 可选：数据类型清理和转换 ---
                        # 根据你的 CSV 数据，你可能需要在这里对 chunk_df 的列进行类型转换
                        # 将 pandas 的 NaT (对于datetime) 和 NaN (对于numeric) 转换为 None，以便 SQLite 识别为 NULL
                        # 将 datetime 对象转换为 ISO8601 字符串格式
                        try:
                             # Convert datetime columns to ISO8601 strings
                             datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pickup_datetime', 'dropoff_datetime']
                             for col in datetime_cols:
                                 if col in chunk_df.columns:
                                     # First, attempt to convert to datetime, coercing errors
                                     chunk_df[col] = pd.to_datetime(chunk_df[col], errors='coerce')
                                     # Then, convert datetime objects to ISO 8601 strings, NaT becomes None
                                     chunk_df[col] = chunk_df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)


                             # Convert numeric columns, handling NaN
                             numeric_cols = ['vendorid', 'passenger_count', 'trip_distance', 'ratecodeid', 'pulocationid',
                                             'dolocationid', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
                                             'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                                             'congestion_surcharge', 'airport_fee'] # Add/remove based on your CSV
                             for col in numeric_cols:
                                  if col in chunk_df.columns:
                                      # First, attempt to convert to numeric, coercing errors
                                      chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')
                                      # Then, replace NaN with None
                                      chunk_df[col] = chunk_df[col].replace({np.nan: None})


                             # Convert boolean columns to INTEGER (0 or 1)
                             # Assuming boolean columns exist and are correctly identified by pandas
                             bool_cols = chunk_df.select_dtypes(include='bool').columns
                             for col in bool_cols:
                                 chunk_df[col] = chunk_df[col].astype(int)


                        except Exception as cast_error:
                             print(f"Warning: Data type casting/conversion error in chunk {chunk_index}: {cast_error}")
                             # Log the error but attempt to insert the chunk anyway


                        print(f"处理块 {chunk_index} ({rows_in_chunk} 行)...")

                        # --- 插入数据块并计时 ---
                        start_time = time.time()
                        # 获取插入前的系统指标 (特别是磁盘 I/O)
                        pre_insert_metrics = get_system_metrics()
                        pre_disk_io = pre_insert_metrics.get('disk_io_counters', None)


                        try:
                            # Convert DataFrame rows to a list of tuples for executemany
                            # .values returns a numpy array, .tolist() converts it to list of lists
                            # Convert inner lists to tuples
                            data_to_insert = [tuple(row) for row in chunk_df.values.tolist()]

                            # Use executemany for efficient insertion of multiple rows
                            cursor.executemany(insert_sql, data_to_insert)
                            conn.commit() # Commit the transaction for this chunk

                        except sqlite3.Error as e:
                             print(f"插入块 {chunk_index} 时发生 SQLite 错误: {e}")
                             # 记录错误日志
                             log_entry = {
                                'timestamp': datetime.now().isoformat(),
                                'chunk_index': chunk_index,
                                'status': 'ERROR',
                                'error': str(e),
                                'rows_attempted': rows_in_chunk,
                                'start_time_utc': start_time,
                                'end_time_utc': time.time(),
                                'system_metrics_at_error': get_system_metrics() # 记录出错时的系统状态
                             }
                             log_f.write(json.dumps(log_entry) + '\n')
                             log_f.flush() # Ensure log is written immediately
                             print(f"  -> 块 {chunk_index} 插入失败。")
                             # Depending on the error, you might want to inspect the data_to_insert for debugging
                             # print(data_to_insert[:5]) # Print first 5 rows of data attempted
                             continue # Skip current chunk and continue with the next
                        except Exception as e:
                             print(f"插入块 {chunk_index} 时发生未预期的错误: {e}")
                             # Record unexpected error
                             log_entry = {
                                'timestamp': datetime.now().isoformat(),
                                'chunk_index': chunk_index,
                                'status': 'UNEXPECTED_ERROR',
                                'error': str(e),
                                'rows_attempted': rows_in_chunk,
                                'start_time_utc': start_time,
                                'end_time_utc': time.time(),
                                'system_metrics_at_error': get_system_metrics() # Record system state at error
                             }
                             log_f.write(json.dumps(log_entry) + '\n')
                             log_f.flush()
                             print(f"  -> 块 {chunk_index} 插入失败。")
                             continue # Skip current chunk and continue with the next


                        end_time = time.time()
                        time_taken_chunk = end_time - start_time
                        # Avoid division by zero if time taken is negligible
                        time_taken_chunk = max(time_taken_chunk, 0.0001)

                        total_time_taken += time_taken_chunk
                        total_rows_ingested += rows_in_chunk

                        # --- 计算速率 ---
                        # 速率 = 行数 / 时间 (秒)
                        ingestion_rate_rows_per_sec = rows_in_chunk / time_taken_chunk

                        # --- 记录块处理后的系统指标 ---
                        post_insert_metrics = get_system_metrics()
                        post_disk_io = post_insert_metrics.get('disk_io_counters', None)


                        # --- 计算本次插入的磁盘 I/O 差值 ---
                        disk_io_delta = {}
                        if pre_disk_io and post_disk_io:
                            # Ensure we are comparing the same type of object
                            if type(pre_disk_io) == type(post_disk_io):
                                disk_io_delta['read_bytes_delta'] = post_disk_io.read_bytes - pre_disk_io.read_bytes
                                disk_io_delta['write_bytes_delta'] = post_disk_io.write_bytes - pre_disk_io.write_bytes
                                disk_io_delta['read_count_delta'] = post_disk_io.read_count - pre_disk_io.read_count
                                disk_io_delta['write_count_delta'] = post_disk_io.write_count - pre_disk_io.write_count
                            else:
                                 print(f"Warning: Disk I/O counter types mismatch in chunk {chunk_index}. Cannot calculate delta.")
                                 disk_io_delta = {'read_bytes_delta': 0, 'write_bytes_delta': 0, 'read_count_delta': 0, 'write_count_delta': 0}
                        else:
                             # If metrics fetching failed, delta is 0
                             disk_io_delta = {'read_bytes_delta': 0, 'write_bytes_delta': 0, 'read_count_delta': 0, 'write_count_delta': 0}


                        # --- 记录日志 ---
                        log_entry = {
                            'timestamp': datetime.now().isoformat(), # Use log write time
                            'chunk_index': chunk_index,
                            'status': 'SUCCESS',
                            'rows_ingested': rows_in_chunk,
                            'time_taken_seconds': round(time_taken_chunk, 4),
                            'ingestion_rate_rows_per_sec': round(ingestion_rate_rows_per_sec, 2),
                            'total_rows_ingested_so_far': total_rows_ingested,
                            'total_time_taken_so_far': round(total_time_taken, 4),
                            'system_metrics_after_chunk': { # Log core system stats after insertion
                                 'cpu_percent': post_insert_metrics.get('cpu_percent', -1),
                                 'memory_percent': post_insert_metrics.get('memory_percent', -1),
                                 'memory_used_gb': post_insert_metrics.get('memory_used_gb', -1),
                                 # You could also log post_insert_metrics['disk_io_counters'] here if needed
                             },
                            'disk_io_delta_during_chunk_bytes': {
                                'read': disk_io_delta['read_bytes_delta'],
                                'write': disk_io_delta['write_bytes_delta']
                            },
                            'disk_io_delta_during_chunk_count': {
                                'read': disk_io_delta['read_count_delta'],
                                'write': disk_io_delta['write_count_delta']
                            }
                        }

                        log_f.write(json.dumps(log_entry) + '\n') # Write one JSON object per line
                        log_f.flush() # Ensure data is written to file immediately

                        # --- Print current progress and rate ---
                        print(f"  -> 完成。耗时: {time_taken_chunk:.4f} 秒，速率: {ingestion_rate_rows_per_sec:.2f} 行/秒。")
                        print(f"  -> 累计插入: {total_rows_ingested} 行，总耗时: {total_time_taken:.4f} 秒。")
                        print(f"  -> CPU: {log_entry['system_metrics_after_chunk'].get('cpu_percent', -1):.1f}%, Mem: {log_entry['system_metrics_after_chunk'].get('memory_percent', -1):.1f}% ({log_entry['system_metrics_after_chunk'].get('memory_used_gb', -1):.2f} GB used)")
                        print(f"  -> Disk I/O (this chunk): Read {disk_io_delta['read_bytes_delta']/1024/1024:.2f} MB, Write {disk_io_delta['write_bytes_delta']/1024/1024:.2f} MB")


                except pd.errors.EmptyDataError:
                    print("CSV 文件为空或格式不正确，或者所有行都被跳过了。")
                except FileNotFoundError:
                    print(f"错误: CSV 文件未找到在 {csv_file}")
                except Exception as e:
                    print(f"读取或处理 CSV 块时发生意外错误: {e}")

            print("\n所有数据块处理完毕。")

        # Database connection is closed automatically when exiting the 'with' block
        print("SQLite 连接已关闭。")

    except sqlite3.Error as e:
        print(f"SQLite 错误: {e}")
    except Exception as e:
        print(f"发生未预期的错误: {e}")

    finally:
        # --- Summary ---
        overall_avg_rate = total_rows_ingested / total_time_taken if total_time_taken > 0 else 0
        print("\n--- 导入总结 (SQLite) ---")
        print(f"总共插入行数: {total_rows_ingested}")
        print(f"总耗时: {total_time_taken:.4f} 秒")
        print(f"整体平均插入速率: {overall_avg_rate:.2f} 行/秒")
        print(f"详细日志已保存到: {log_file}")
        print("请检查日志文件分析插入速率，并结合系统监控数据（CPU、内存、磁盘 I/O）。")


# --- Run script ---
if __name__ == "__main__":
    ingest_and_monitor_sqlite(csv_file, db_file, table_name, log_file, chunk_size)
