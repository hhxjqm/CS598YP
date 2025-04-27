import argparse
import json
import pandas as pd
import matplotlib.pyplot as plt
import os
import matplotlib.dates as mdates

def plot_streamwrite_log(log_file, output_path, title='StreamWrite Ingestion Overview', width=15, height=12):
    # --- 确保输出目录存在 ---
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"✅ 创建输出目录: {output_dir}")

    # --- 读取日志文件 ---
    log_data = []
    try:
        print(f"📖 正在读取日志文件: {log_file}")
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    log_entry = json.loads(line)
                    if log_entry.get('status') == 'SUCCESS':  # 只保留成功的记录
                        log_data.append(log_entry)
                except json.JSONDecodeError:
                    continue  # 解析失败直接跳过
    except FileNotFoundError:
        print(f"❌ 找不到日志文件: {log_file}")
        return

    if not log_data:
        print("⚠️ 没有找到有效的 SUCCESS 日志记录。")
        return

    # --- 转为 DataFrame ---
    df = pd.DataFrame(log_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # --- 只保留最近一小时的数据 ---
    cutoff = df['timestamp'].max() - pd.Timedelta(hours=1)
    df = df[df['timestamp'] >= cutoff]

    if df.empty:
        print("⚠️ 最近一小时没有数据点。")
        return

    # --- 提取系统指标 ---
    df['cpu'] = df['cpu_percent']  # 🔥 cpu在最外层，直接取！
    df['memory'] = df['system_metrics'].apply(lambda x: x.get('memory_percent', -1))  # memory还是要在system_metrics里找
    df['disk_read_mb'] = df['system_metrics'].apply(lambda x: x.get('disk_io_counters', {}).get('read_bytes', 0) / (1024 * 1024))
    df['disk_write_mb'] = df['system_metrics'].apply(lambda x: x.get('disk_io_counters', {}).get('write_bytes', 0) / (1024 * 1024))

    # --- 平滑曲线 (移动平均Moving Average) ---
    window_size = 20  # 移动平均窗口大小
    df['ingestion_rate_smooth'] = df['ingestion_rate_rows_per_sec'].rolling(window=window_size, min_periods=1).mean()
    df['cpu_smooth'] = df['cpu'].rolling(window=window_size, min_periods=1).mean()
    df['memory_smooth'] = df['memory'].rolling(window=window_size, min_periods=1).mean()
    df['disk_read_smooth'] = df['disk_read_mb'].rolling(window=window_size, min_periods=1).mean()
    df['disk_write_smooth'] = df['disk_write_mb'].rolling(window=window_size, min_periods=1).mean()

    # --- 开始绘图 ---
    print("📊 正在绘图...")
    fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(width, height), sharex=True)

    # --- 图1：插入速率 (Rows/sec) ---
    axes[0].scatter(df['timestamp'], df['ingestion_rate_rows_per_sec'], alpha=0.6, s=10, label='Rows/sec')
    axes[0].plot(df['timestamp'], df['ingestion_rate_smooth'], color='black', linestyle='-', label='Rows/sec Moving Avg')
    axes[0].set_ylabel('Rows/sec')
    axes[0].set_title(title)
    axes[0].grid(True)
    axes[0].legend(bbox_to_anchor=(1.05, 1.0), loc='upper left', fontsize='small')

    # --- 图2：CPU 使用率 (CPU %) ---
    axes[1].scatter(df['timestamp'], df['cpu'], alpha=0.6, s=10, color='orange', label='CPU %')
    axes[1].plot(df['timestamp'], df['cpu_smooth'], color='black', linestyle='-', label='CPU Moving Avg')
    axes[1].set_ylabel('CPU %')
    axes[1].grid(True)
    axes[1].legend(bbox_to_anchor=(1.05, 0.8), loc='upper left', fontsize='small')

    # --- 图3：内存使用率 (Memory %) ---
    axes[2].scatter(df['timestamp'], df['memory'], alpha=0.6, s=10, color='red', label='Memory %')
    axes[2].plot(df['timestamp'], df['memory_smooth'], color='black', linestyle='-', label='Memory Moving Avg')
    axes[2].set_ylabel('Memory %')
    axes[2].grid(True)
    axes[2].legend(bbox_to_anchor=(1.05, 0.6), loc='upper left', fontsize='small')

    # --- 图4：磁盘读 (Disk Read MB) ---
    axes[3].scatter(df['timestamp'], df['disk_read_mb'], alpha=0.6, s=10, color='green', label='Disk Read (MB)')
    axes[3].plot(df['timestamp'], df['disk_read_smooth'], color='black', linestyle='-', label='Disk Read Moving Avg')
    axes[3].set_ylabel('Disk Read (MB)')
    axes[3].grid(True)
    axes[3].legend(bbox_to_anchor=(1.05, 0.6), loc='upper left', fontsize='small')

    # --- 图5：磁盘写 (Disk Write MB) ---
    axes[4].scatter(df['timestamp'], df['disk_write_mb'], alpha=0.6, s=10, color='purple', label='Disk Write (MB)')
    axes[4].plot(df['timestamp'], df['disk_write_smooth'], color='black', linestyle='-', label='Disk Write Moving Avg')
    axes[4].set_ylabel('Disk Write (MB)')
    axes[4].set_xlabel('Time')
    axes[4].grid(True)
    axes[4].legend(bbox_to_anchor=(1.05, 0.5), loc='upper left', fontsize='small')

    # --- 时间格式美化 ---
    axes[4].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[4].xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()

    # --- 布局调整 ---
    plt.tight_layout()

    # --- 保存图表 ---
    plt.savefig(output_path, bbox_inches='tight')
    print(f"✅ 图表保存成功: {output_path}")

# --- CLI 入口 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StreamWrite 写入日志可视化工具")
    parser.add_argument('--log', required=True, help='日志文件路径 (.jsonl)')
    parser.add_argument('--out', required=True, help='输出图路径 (.png)')
    parser.add_argument('--title', default='StreamWrite Ingestion Overview', help='图表标题')
    parser.add_argument('--width', type=int, default=15, help='图宽')
    parser.add_argument('--height', type=int, default=12, help='图高')

    args = parser.parse_args()

    plot_streamwrite_log(
        log_file=args.log,
        output_path=args.out,
        title=args.title,
        width=args.width,
        height=args.height
    )
