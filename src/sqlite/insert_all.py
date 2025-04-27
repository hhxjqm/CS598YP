import sqlite3
import pandas as pd
import os
import time

# --- 配置 ---
csv_file = 'data_set/2023_Yellow_Taxi_Trip_Data.csv'
db_file = 'db/yellow_taxi.sqlite'
table_name = 'yellow_taxi_trips'
chunksize = 200000  # 提大到20万行一块

# --- 确保路径存在 ---
os.makedirs(os.path.dirname(db_file), exist_ok=True)

# --- 连接SQLite ---
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# --- 提速配置 ---
cursor.execute("PRAGMA synchronous = OFF;")
cursor.execute("PRAGMA journal_mode = MEMORY;")
cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
conn.commit()

print("✅ SQLite配置完成，加速模式开启")

# --- 推断列结构 ---
sample_df = pd.read_csv(csv_file, nrows=10, low_memory=False)
sample_df.columns = sample_df.columns.str.lower()

# --- 手动字段类型定义（注意时间列是TEXT）---
dtype_mapping = {
    'vendorid': 'INTEGER',
    'tpep_pickup_datetime': 'TEXT',
    'tpep_dropoff_datetime': 'TEXT',
    'passenger_count': 'INTEGER',
    'trip_distance': 'REAL',
    'ratecodeid': 'INTEGER',
    'store_and_fwd_flag': 'TEXT',
    'pulocationid': 'INTEGER',
    'dolocationid': 'INTEGER',
    'payment_type': 'INTEGER',
    'fare_amount': 'REAL',
    'extra': 'REAL',
    'mta_tax': 'REAL',
    'tip_amount': 'REAL',
    'tolls_amount': 'REAL',
    'improvement_surcharge': 'REAL',
    'total_amount': 'REAL',
    'congestion_surcharge': 'REAL',
    'airport_fee': 'REAL'
}

columns_sql = []
for col in sample_df.columns:
    sqlite_type = dtype_mapping.get(col, 'TEXT')
    columns_sql.append(f'"{col}" {sqlite_type}')

create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)});"
cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
cursor.execute(create_table_sql)
conn.commit()
print(f"✅ 表 {table_name} 创建成功")

# --- 批量插入SQL ---
insert_sql = f"INSERT INTO {table_name} ({', '.join(sample_df.columns)}) VALUES ({', '.join(['?'] * len(sample_df.columns))});"

# --- 开启一个大事务 ---
conn.execute("BEGIN TRANSACTION;")
print("🚀 开始批量插入...")

start_time = time.time()
total_inserted = 0

# --- 分块读取并插入 ---
for chunk in pd.read_csv(csv_file, chunksize=chunksize, low_memory=False):
    chunk.columns = chunk.columns.str.lower()

    # 处理时间列格式（加速）
    datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in datetime_cols:
        if col in chunk.columns:
            chunk[col] = pd.to_datetime(chunk[col], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
            chunk[col] = chunk[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 空值统一处理为None
    chunk = chunk.where(pd.notnull(chunk), None)

    # 批量插入
    cursor.executemany(insert_sql, [tuple(row) for row in chunk.values])
    total_inserted += len(chunk)

    elapsed = time.time() - start_time
    speed = total_inserted / elapsed if elapsed > 0 else 0
    print(f"✅ 当前总插入: {total_inserted} 行, 平均速度: {speed:.2f} 行/秒")

# --- 提交整个事务 ---
conn.commit()
conn.close()

total_time = time.time() - start_time
print(f"\n✅ 所有数据插入完成")
print(f"✅ 总插入行数: {total_inserted}")
print(f"✅ 总耗时: {total_time:.2f} 秒")
print(f"✅ 平均速度: {total_inserted / total_time:.2f} 行/秒")
