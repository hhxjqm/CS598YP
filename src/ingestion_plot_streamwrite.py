import argparse
import json
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime
import matplotlib.dates as mdates

def plot_streamwrite_log(log_file, output_path, title='StreamWrite Ingestion - 1 Hour Test', width=15, height=8):
    # --- 确保图表输出目录存在 ---
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"✅ Created output directory: {output_dir}")

    # --- 读取日志 ---
    log_data = []
    try:
        print(f"📖 Reading log file: {log_file}")
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    log_entry = json.loads(line)
                    if log_entry.get('status') == 'SUCCESS':
                        log_data.append(log_entry)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        print(f"❌ Log file not found: {log_file}")
        return

    if not log_data:
        print("⚠️ No valid SUCCESS logs found.")
        return

    # --- 转为 DataFrame ---
    df = pd.DataFrame(log_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # --- 仅保留最近一小时数据 ---
    cutoff = df['timestamp'].max() - pd.Timedelta(hours=1)
    df = df[df['timestamp'] >= cutoff]

    if df.empty:
        print("⚠️ No data points in the last hour.")
        return

    # --- 计算系统指标 ---
    df['cpu'] = df['system_metrics'].apply(lambda x: x.get('cpu_percent', -1))
    df['memory'] = df['system_metrics'].apply(lambda x: x.get('memory_percent', -1))
    df['disk_read_mb'] = df['system_metrics'].apply(lambda x: x.get('disk_io_counters', {}).get('read_bytes', 0) / (1024 * 1024))
    df['disk_write_mb'] = df['system_metrics'].apply(lambda x: x.get('disk_io_counters', {}).get('write_bytes', 0) / (1024 * 1024))

    # --- 平滑 Rows/sec 曲线 ---
    df['ingestion_rate_smooth'] = df['ingestion_rate_rows_per_sec'].rolling(window=5).mean()

    # --- 开始绘图 ---
    print("📊 Plotting...")
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(width, height), sharex=True)

    # --- 子图 1：插入速率 ---
    axes[0].plot(df['timestamp'], df['ingestion_rate_smooth'], label='Smoothed Rows/sec', color='blue', linewidth=1.2)
    axes[0].set_ylabel('Rows/sec')
    axes[0].set_title(title)
    axes[0].grid(True, linestyle='--', alpha=0.6)
    axes[0].legend(loc='upper left')

    # --- 子图 2：系统指标 ---
    axes[1].plot(df['timestamp'], df['cpu'], label='CPU (%)', color='orange')
    axes[1].plot(df['timestamp'], df['memory'], label='Memory (%)', color='red')
    axes[1].plot(df['timestamp'], df['disk_read_mb'], label='Disk Read (MB)', color='green', alpha=0.7)
    axes[1].plot(df['timestamp'], df['disk_write_mb'], label='Disk Write (MB)', color='purple', alpha=0.7)

    axes[1].set_ylabel('Utilization / Disk I/O')
    axes[1].set_xlabel('Time')
    axes[1].grid(True, linestyle='--', alpha=0.6)
    axes[1].legend(loc='upper left')

    # --- 设置时间格式化器 ---
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches='tight')
    print(f"✅ Plot saved to: {output_path}")

# --- CLI 入口 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize StreamWrite ingestion logs")
    parser.add_argument('--log', required=True, help='Path to input .jsonl log file')
    parser.add_argument('--out', required=True, help='Path to save output plot (.png)')
    parser.add_argument('--title', default='StreamWrite Ingestion - 1 Hour Test', help='Title of the plot')
    parser.add_argument('--width', type=int, default=15, help='Plot width in inches')
    parser.add_argument('--height', type=int, default=8, help='Plot height in inches')

    args = parser.parse_args()

    plot_streamwrite_log(
        log_file=args.log,
        output_path=args.out,
        title=args.title,
        width=args.width,
        height=args.height
    )
