import argparse
import json
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime

def plot_streamwrite_log(log_file, output_path, title='StreamWrite Metrics', width=15, height=10):
    # --- 确保图表输出目录存在 ---
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"✅ 创建图表输出目录: {output_dir}")

    # --- 读取日志 ---
    log_data = []
    try:
        print(f"📖 正在读取日志文件: {log_file}")
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    log_entry = json.loads(line)
                    if log_entry.get('status') == 'SUCCESS':
                        log_data.append(log_entry)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        print(f"❌ 日志文件未找到: {log_file}")
        return

    if not log_data:
        print("⚠️ 日志中没有可用的数据点。")
        return

    # --- 转为 DataFrame ---
    df = pd.DataFrame(log_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # --- 计算CPU/MEM指标 ---
    df['cpu'] = df['system_metrics'].apply(lambda x: x.get('cpu_percent', -1))
    df['memory'] = df['system_metrics'].apply(lambda x: x.get('memory_percent', -1))

    # --- 绘图 ---
    print("📊 正在生成图表...")
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(width, height), sharex=True)

    # 子图 1：写入速率
    axes[0].plot(df['timestamp'], df['ingestion_rate_rows_per_sec'], label='Rows/sec', color='blue')
    axes[0].set_ylabel('Rows/sec')
    axes[0].set_title(title)
    axes[0].grid(True, linestyle='--', alpha=0.6)
    axes[0].legend(loc='upper left')

    # 子图 2：系统指标
    axes[1].plot(df['timestamp'], df['cpu'], label='CPU (%)', color='orange')
    axes[1].plot(df['timestamp'], df['memory'], label='Memory (%)', color='red')
    axes[1].set_ylabel('Utilization (%)')
    axes[1].set_xlabel('Time')
    axes[1].grid(True, linestyle='--', alpha=0.6)
    axes[1].legend(loc='upper left')

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches='tight')
    print(f"✅ 图表已保存: {output_path}")

# --- CLI 入口 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="可视化 StreamWrite 日志")
    parser.add_argument('--log', required=True, help='输入日志路径（.jsonl）')
    parser.add_argument('--out', required=True, help='输出图表路径（.png）')
    parser.add_argument('--title', default='StreamWrite Metrics', help='图表标题')
    parser.add_argument('--width', type=int, default=15, help='图宽')
    parser.add_argument('--height', type=int, default=10, help='图高')

    args = parser.parse_args()

    plot_streamwrite_log(
        log_file=args.log,
        output_path=args.out,
        title=args.title,
        width=args.width,
        height=args.height
    )
