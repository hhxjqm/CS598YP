import json
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

# --- 配置参数 ---
log_dir = 'log'  # 保存 .jsonl 日志的目录
output_plot_path = 'plots/ingestion_rate_comparison.png'

# --- 确保输出目录存在 ---
output_dir = os.path.dirname(output_plot_path)
if output_dir and not os.path.exists(output_dir):
    os.makedirs(output_dir, exist_ok=True)

# --- 扫描所有 log 文件 ---
log_files = glob.glob(os.path.join(log_dir, 'ingestion_log_*.jsonl'))

if not log_files:
    print(f"未找到日志文件在 {log_dir}")
    exit()

# --- 准备绘图 ---
plt.figure(figsize=(15, 8))

for log_file in log_files:
    experiment_name = os.path.basename(log_file).replace('ingestion_log_', '').replace('.jsonl', '')
    print(f"加载实验日志: {experiment_name}")

    log_data = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                log_entry = json.loads(line)
                if log_entry.get('status') == 'SUCCESS':
                    log_data.append(log_entry)
            except json.JSONDecodeError:
                continue

    if not log_data:
        print(f"警告: {experiment_name} 没有成功的插入记录，跳过。")
        continue

    df = pd.DataFrame(log_data)
    df['chunk_index'] = df['chunk_index'].astype(int)
    df = df.sort_values('chunk_index')

    plt.plot(df['chunk_index'], df['ingestion_rate_rows_per_sec'], label=experiment_name)

# --- 设置图表属性 ---
plt.title('DuckDB Ingestion Rate Comparison (Different Memory Limits and Threads)')
plt.xlabel('Chunk Index')
plt.ylabel('Ingestion Rate (rows/sec)')
plt.legend(title='Experiment', loc='upper right')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()

# --- 保存图表 ---
plt.savefig(output_plot_path, bbox_inches='tight')
print(f"✅ 图表已保存到: {output_plot_path}")

# 如果你想直接弹出窗口看图，可以取消下面这行注释
# plt.show()
