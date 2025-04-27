import duckdb
import time
import os
import json
from datetime import datetime
import psutil
import pandas as pd # 仅用于可能的辅助功能，主要用 duckdb

# --- 配置参数 ---
# DuckDB 数据库文件路径 (使用之前脚本创建的数据库)
db_file = 'db/taxi_data.duckdb'
# 日志文件路径
log_file = 'log/operator_latency_log.jsonl' # 新的日志文件
# 目标表名 (与 ingest 脚本一致)
table_name = 'yellow_taxi_trips'
# DuckDB Profiling 模式 ('json' 或 'query_tree')
# 'json' 提供了详细的结构化数据，便于解析
profiling_mode = 'json'

# --- 示例交互式查询 ---
# 这些查询应该在你的 taxi 数据集上是有意义的
# 选择一些有代表性的操作: 点查询, 范围查询, 简单聚合, 带过滤的聚合, JOIN (如果适用)
sample_queries = [
    # 1. 简单计数
    f"SELECT COUNT(*) FROM {table_name};",
    # 2. 按付款类型分组计数 (聚合)
    f"SELECT payment_type, COUNT(*) FROM {table_name} GROUP BY payment_type;",
    # 3. 计算平均行程距离 (聚合 + 过滤)
    f"SELECT AVG(trip_distance) FROM {table_name} WHERE passenger_count > 1;",
    # 4. 查找特定时间范围内的行程 (过滤 + 排序 + 限制) - 假设有 tpep_pickup_datetime 列
    # 注意: 日期时间格式需要与你的数据匹配
    # 如果你的时间戳是字符串, 可能需要 CAST(tpep_pickup_datetime AS TIMESTAMP)
    f"SELECT vendorid, trip_distance, total_amount FROM {table_name} WHERE tpep_pickup_datetime BETWEEN '2023-01-15 08:00:00' AND '2023-01-15 09:00:00' ORDER BY total_amount DESC LIMIT 10;",
    # 5. 查找特定地点之间的行程 (多重过滤)
    f"SELECT trip_distance, fare_amount, tip_amount FROM {table_name} WHERE PULocationID = 100 AND DOLocationID = 200 AND passenger_count = 2 LIMIT 5;",
    # 6. 计算不同供应商的平均小费金额 (聚合 + 过滤)
    f"SELECT vendorid, AVG(tip_amount) FROM {table_name} WHERE tip_amount > 0 GROUP BY vendorid;",
    # 7. (如果适用) JOIN 查询示例 - 如果有另一个表，例如 'zones'
    # f"SELECT t.trip_distance, zpu.zone as pickup_zone, zdo.zone as dropoff_zone
    #  FROM {table_name} t
    #  JOIN zones zpu ON t.PULocationID = zpu.locationid
    #  JOIN zones zdo ON t.DOLocationID = zdo.locationid
    #  WHERE t.passenger_count = 1 LIMIT 5;",
]

# --- 确保目录存在 ---
log_dir = os.path.dirname(log_file)
os.makedirs(log_dir, exist_ok=True)
print(f"确保日志目录存在: {log_dir}")

# --- 获取系统资源信息的函数 (与 ingest 脚本相同) ---
def get_system_metrics():
    """获取当前的系统资源使用情况"""
    metrics = {}
    try:
        metrics['cpu_percent'] = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        metrics['memory_percent'] = mem.percent
        metrics['memory_used_gb'] = round(mem.used / (1024**3), 2)
        metrics['disk_io_counters'] = psutil.disk_io_counters()._asdict() # 转为字典方便 JSON 序列化
    except Exception as e:
        print(f"获取系统指标时发生错误: {e}")
        metrics['error'] = str(e)
    return metrics

# --- 解析 DuckDB JSON Profiling 输出的函数 ---
def parse_profiling_json(json_str):
    """
    解析 DuckDB profiling JSON 输出，提取操作符信息和计时。
    DuckDB JSON profiling 输出结构可能变化，这里基于常见模式解析。
    主要关注 'result', 'timings', 'children' 和 'name' 字段。
    """
    operator_timings = []
    total_query_time_ms = 0.0 # 从 profiling 数据中获取的总时间

    # Handle empty input string
    if not json_str:
        print("Warning: parse_profiling_json received empty input.")
        return [], 0.0

    try:
        profile_data = json.loads(json_str)

        # 记录从 profiling 元数据中提取的总查询时间
        if 'timing' in profile_data:
             total_query_time_ms = profile_data.get('timing', 0.0) * 1000 # 假设单位是秒

        # 递归函数来遍历查询计划树
        def traverse_node(node):
            operator_info = {}
            if 'name' in node and 'timing' in node:
                # 将 timing (秒) 转换为毫秒
                duration_ms = node['timing'] * 1000
                operator_info = {
                    'operator_name': node['name'],
                    'duration_ms': round(duration_ms, 4),
                    # 'cardinality': node.get('cardinality', -1), # 基数/结果行数
                    # 添加其他你感兴趣的指标，例如 'extra_info'
                    'extra_info': node.get('extra_info', None)
                }
                operator_timings.append(operator_info)

            # 递归处理子节点
            if 'children' in node and isinstance(node['children'], list):
                for child in node['children']:
                    traverse_node(child)

        # 从根节点开始遍历 (DuckDB profiling JSON 通常有一个根节点)
        if 'result' in profile_data: # 新版 DuckDB 似乎将 profiling 嵌套在 result 里？
             # 需要确认实际 JSON 结构，这里假设根在 profile_data 或 profile_data['result']
             root_node = profile_data # 或者可能是 profile_data['tree'] 或其他键
             # 如果 profile_data 直接是 list, 可能需要迭代处理
             if isinstance(root_node, dict):
                 traverse_node(root_node)
             elif isinstance(root_node, list): # 有时根可能是列表
                  for item in root_node:
                      if isinstance(item, dict):
                          traverse_node(item)
        elif isinstance(profile_data, dict): # 兼容直接是根节点的情况
             traverse_node(profile_data)


    except json.JSONDecodeError as e:
        print(f"解析 profiling JSON 时出错: {e}")
        print(f"原始 JSON 字符串: {json_str[:500]}...") # 打印部分原始字符串帮助调试
        operator_timings.append({'error': 'JSONDecodeError', 'message': str(e)})
    except Exception as e:
        print(f"处理 profiling 数据时发生未知错误: {e}")
        operator_timings.append({'error': 'ProcessingError', 'message': str(e)})

    # 返回解析出的操作符计时列表和从profile获取的总时间
    return operator_timings, total_query_time_ms


# --- 主分析函数 ---
def profile_queries(db_path, queries, log_path, prof_mode):
    """连接数据库，运行查询，记录性能和操作符延迟"""
    print(f"开始分析查询，数据库: {db_path}")
    print(f"日志将记录到: {log_path}")
    profile_temp_file = './duckdb_profile_temp.json' # 使用当前目录

    try:
        # 连接到 DuckDB 数据库
        with duckdb.connect(database=db_path, read_only=True) as con: # 以只读模式打开，避免意外修改
            print("成功连接到 DuckDB 数据库。")

            # 启用 profiling 并指定输出文件
            try:
                con.execute(f"PRAGMA enable_profiling='{prof_mode}';")
                con.execute(f"PRAGMA profile_output='{profile_temp_file}';")
                print(f"已启用 DuckDB profiling 模式: '{prof_mode}'")
                print(f"Profiling 输出将写入: '{profile_temp_file}'")
            except duckdb.Error as e:
                print(f"无法启用 profiling 或设置输出文件: {e}.")
                return

            # 打开日志文件准备写入
            with open(log_path, 'a', encoding='utf-8') as log_f:
                # 运行每个示例查询
                for i, query in enumerate(queries):
                    print(f"\n--- 运行查询 {i+1}/{len(queries)} ---")
                    print(f"查询语句: {query}")

                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'query_index': i + 1,
                        'query': query,
                        'status': 'UNKNOWN',
                        'system_metrics_before': get_system_metrics()
                    }

                    # 清理上一次查询可能留下的 profile 文件 (如果存在)
                    if os.path.exists(profile_temp_file):
                         try:
                             os.remove(profile_temp_file)
                         except OSError as e:
                             print(f"Warning: 无法删除旧的 profile 文件 {profile_temp_file}: {e}")


                    try:
                        # --- 执行查询并计时 ---
                        start_time = time.perf_counter()
                        result = con.execute(query).fetchall() # 执行目标查询
                        end_time = time.perf_counter()
                        total_time_taken_sec = end_time - start_time

                        # Add a small delay to allow file writing
                        time.sleep(0.1)

                        # --- 获取 profiling 数据 (修改之处) ---
                        profiling_output_raw = "" # Store raw output here
                        profiling_read_error = None
                        try:
                            if os.path.exists(profile_temp_file):
                                with open(profile_temp_file, 'r', encoding='utf-8') as pf:
                                    profiling_output_raw = pf.read()
                                print(f"--- Debug: Raw profiling output read for query {i+1} ---")
                                print(profiling_output_raw[:500] + ("..." if len(profiling_output_raw) > 500 else ""))
                                print("--- End Debug ---")
                            else:
                                print(f"Warning: Profile 文件 {profile_temp_file} 未找到。")
                                profiling_read_error = f"Profile file not found: {profile_temp_file}"
                        except IOError as e:
                            print(f"读取 profile 文件时出错 {profile_temp_file}: {e}")
                            profiling_read_error = f"Error reading profile file: {e}"

                        # --- 解析 profiling 数据 (REMOVED FOR NOW) ---
                        # operator_details, profile_total_time_ms = parse_profiling_json(profiling_output_raw)
                        # We will parse later based on the raw output logged

                        # --- 记录结果 (Modified) ---
                        log_entry.update({
                            'status': 'SUCCESS',
                            'total_time_seconds': round(total_time_taken_sec, 6),
                            # 'profiling_total_time_ms': round(profile_total_time_ms, 4), # Removed
                            # 'operator_timings': operator_details, # Removed
                            'num_result_rows': len(result) if result else 0,
                            'raw_profile_json': profiling_output_raw, # Log the raw string
                            'profiling_error': profiling_read_error, # Log read error if any
                            'system_metrics_after': get_system_metrics()
                        })
                        print(f"  -> 查询成功完成。")
                        print(f"  -> 总耗时 (Python 측정): {total_time_taken_sec:.6f} 秒")
                        # if profile_total_time_ms > 0: # Removed
                        #     print(f"  -> 总耗时 (Profile): {profile_total_time_ms:.4f} 毫秒")
                        print(f"  -> 返回行数: {log_entry['num_result_rows']}")
                        print(f"  -> Raw profile JSON logged (first 50 chars): {profiling_output_raw[:50]}") # Indicate raw data was logged
                        # print(f"  -> 操作符耗时详情:") # Removed
                        # for op in operator_details:
                        #     if 'error' not in op:
                        #         print(f"      - {op.get('operator_name', 'Unknown')}: {op.get('duration_ms', 'N/A')} ms {op.get('extra_info', '')}")
                        #     else:
                        #         print(f"      - 解析错误: {op.get('message', '')}")

                    except duckdb.Error as e:
                        print(f"  -> 执行查询时发生 DuckDB 错误: {e}")
                        log_entry['status'] = 'ERROR'
                        log_entry['error_message'] = str(e)
                        log_entry['system_metrics_after'] = get_system_metrics()

                    except Exception as e:
                        print(f"  -> 执行查询时发生未知错误: {e}")
                        log_entry['status'] = 'ERROR'
                        log_entry['error_message'] = str(e)
                        log_entry['system_metrics_after'] = get_system_metrics()
                    finally:
                        # 将日志条目写入文件
                        log_f.write(json.dumps(log_entry) + '\n')
                        log_f.flush()

                        # 尝试清理本次查询的 profile 文件
                        if os.path.exists(profile_temp_file):
                           try:
                               os.remove(profile_temp_file)
                           except OSError as e:
                               print(f"Warning: 无法在 finally 中删除 profile 文件 {profile_temp_file}: {e}")

            print("\n所有查询分析完毕。")

        # 数据库连接在退出 'with' 块时自动关闭
        print("DuckDB 连接已关闭。")

    except duckdb.Error as e:
        print(f"连接数据库时发生 DuckDB 错误: {e}")
    except FileNotFoundError:
        print(f"错误: 数据库文件未找到在 {db_path}。请先运行数据导入脚本。")
    except Exception as e:
        print(f"发生未预期的错误: {e}")

# --- 运行脚本 ---
if __name__ == "__main__":
    # 检查数据库文件是否存在
    if not os.path.exists(db_file):
        print(f"错误: 数据库文件 '{db_file}' 不存在。")
        print("请确保你已经运行了数据导入脚本 (例如 ingestion_duckdb.py) 来创建和填充数据库。")
    else:
        profile_queries(db_file, sample_queries, log_file, profiling_mode)