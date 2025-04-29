import argparse
import json
import pandas as pd
import matplotlib.pyplot as plt
import os

# --- 查询性能可视化：支持 CPU、内存、query_type 分类 ---
# def plot_query_log(log_file, output_path, title='Query Performance Overview', width=15, height=12):
#     with open(log_file, 'r', encoding='utf-8') as f:
#         records = [json.loads(line) for line in f if line.strip()]
#     df = pd.DataFrame(records)
#     df['query_id'] = range(1, len(df) + 1)
#     df['timestamp'] = pd.to_datetime(df['timestamp'])
#
#     fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(width, height), sharex=True)
#
#     # --- 图1：查询延迟 ---
#     for qtype, group in df.groupby('query_type'):
#         axes[0].plot(group['query_id'], group['time_taken_seconds'], label=qtype)
#     axes[0].set_ylabel('Latency (s)')
#     axes[0].legend()
#     axes[0].set_title(title)
#     axes[0].grid(True)
#
#     # --- 图2：返回行数 ---
#     for qtype, group in df.groupby('query_type'):
#         axes[1].plot(group['query_id'], group['row_count'], label=qtype)
#     axes[1].set_ylabel('Rows')
#     axes[1].legend()
#     axes[1].grid(True)
#
#     # --- 图3：CPU 使用率 ---
#     axes[2].plot(df['query_id'], df['cpu_percent'], color='orange', label='CPU Usage %')
#     axes[2].set_ylabel('CPU %')
#     axes[2].legend()
#     axes[2].grid(True)
#
#     # --- 图4：内存使用率 ---
#     axes[3].plot(df['query_id'], df['memory_percent'], color='purple', label='Memory Usage %')
#     axes[3].set_ylabel('Memory %')
#     axes[3].set_xlabel('Query #')
#     axes[3].legend()
#     axes[3].grid(True)
#
#     plt.tight_layout()
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
#     plt.savefig(output_path, bbox_inches='tight')
#     print(f"✅ 图表保存完成：{output_path}")

def plot_query_log(log_file, output_path, title='Query Performance Overview', width=15, height=12):
    # 读取日志
    with open(log_file, 'r', encoding='utf-8') as f:
        records = [json.loads(line) for line in f if line.strip()]
    df = pd.DataFrame(records)
    df['query_id'] = range(1, len(df) + 1)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 创建子图
    fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(width, height), sharex=True)

    # --- 图1：延迟Latency ---
    for qtype, group in df.groupby('query_type'):
        axes[0].scatter(group['query_id'], group['time_taken_seconds'], alpha=0.6, label=qtype, s=10)
    axes[0].plot(df['query_id'], df['time_taken_seconds'].rolling(window=20, min_periods=1).mean(),
                 color='black', linestyle='-', label='Latency Moving Avg')
    axes[0].set_ylabel('Latency (s)')
    axes[0].set_title(title)
    axes[0].grid(True)
    axes[0].legend(bbox_to_anchor=(1.05, 1.0), loc='upper left', fontsize='small', ncol=2)  # 🔥 近一点

    # --- 图2：返回行数Rows ---
    for qtype, group in df.groupby('query_type'):
        axes[1].scatter(group['query_id'], group['row_count'], alpha=0.6, label=qtype, s=10)
    axes[1].plot(df['query_id'], df['row_count'].rolling(window=20, min_periods=1).mean(),
                 color='black', linestyle='-', label='Rows Moving Avg')
    axes[1].set_ylabel('Rows')
    axes[1].set_yscale('log')
    axes[1].grid(True)
    axes[1].legend(bbox_to_anchor=(1.05, 0.5), loc='upper left', fontsize='small', ncol=2)  # 🔥 近一点

    # --- 图3：CPU 使用率 ---
    axes[2].scatter(df['query_id'], df['cpu_percent'], color='orange', alpha=0.6, s=10, label='CPU Usage %')
    axes[2].plot(df['query_id'], df['cpu_percent'].rolling(window=20, min_periods=1).mean(),
                 color='black', linestyle='-', label='CPU Moving Avg')
    axes[2].set_ylabel('CPU %')
    axes[2].legend()
    axes[2].grid(True)

    # --- 图4：内存使用率 ---
    memory_smooth = df['memory_percent'].rolling(window=20, min_periods=1).mean()
    axes[3].plot(df['query_id'], memory_smooth, color='black', linestyle='-', label='Memory Moving Avg')
    axes[3].set_ylabel('Memory %')
    axes[3].set_xlabel('Query #')
    axes[3].legend()
    axes[3].grid(True)

    # --- 布局调整 ---
    # plt.subplots_adjust(right=0.85, hspace=0.4)  # 🔥 主体拉宽 + 子图留距离
    # plt.tight_layout()

    # 保存
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, bbox_inches='tight')
    print(f"✅ 图表保存完成：{output_path}")



# --- CLI 入口 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckDB 查询日志可视化工具")
    parser.add_argument('--log', required=True, help='日志文件路径 (.jsonl)')
    parser.add_argument('--out', required=True, help='输出图路径 (.png)')
    parser.add_argument('--title', default='Query Performance Overview', help='图标题')
    parser.add_argument('--width', type=int, default=15, help='图宽')
    parser.add_argument('--height', type=int, default=12, help='图高')

    args = parser.parse_args()

    plot_query_log(
        log_file=args.log,
        output_path=args.out,
        title=args.title,
        width=args.width,
        height=args.height
    )
