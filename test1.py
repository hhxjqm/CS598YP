import pandas as pd

# 替换成你的 CSV 文件路径
file_path = 'data_set/2023_Yellow_Taxi_Trip_Data.csv' # <-- 修改这里

# 指定要读取的前几行数量，例如读取前 10 行
num_rows_to_read = 10

try:
    # 使用 nrows 参数只读取指定数量的行
    # header=True 是默认值，表示第一行是列名
    df_head = pd.read_csv(file_path, nrows=num_rows_to_read)

    print(f"成功读取文件前 {num_rows_to_read} 行。")
    print("\n--- 文件信息 (仅包含前几行) ---")
    # 注意：info() 基于这几行推断类型，可能不代表整个文件
    df_head.info()

    print("\n--- 前几行数据 ---")
    # head() 默认显示前 5 行，但因为 df_head 只有 num_rows_to_read 行，
    # 它会显示 df_head 的所有行 (不超过 num_rows_to_read)
    print(df_head.head(num_rows_to_read))


except FileNotFoundError:
    print(f"错误：文件未找到，请检查路径是否正确: {file_path}")
except Exception as e:
    print(f"读取文件时发生错误: {e}")