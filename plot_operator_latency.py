import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- 配置 ---
log_file = 'log/operator_latency_log.jsonl'
output_dir = 'plots'
plot_filename = 'operator_latency_plots.png'

# --- 确保绘图目录存在 ---
os.makedirs(output_dir, exist_ok=True)
print(f"Plots will be saved to directory: {output_dir}")

# --- 加载日志数据 ---
data = []
try:
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping unparseable log line: {e} - Line content: {line.strip()}")
except FileNotFoundError:
    print(f"Error: Log file not found: {log_file}")
    exit()
except Exception as e:
    print(f"Error reading log file: {e}")
    exit()

if not data:
    print("Error: No valid data found in the log file.")
    exit()

# 转换为 Pandas DataFrame
df = pd.DataFrame(data)

# --- 数据预处理 ---
# 仅保留成功执行的查询记录
df_success = df[df['status'] == 'SUCCESS'].copy()

if df_success.empty:
    print("Error: No successful query records found in the log for plotting.")
    exit()

# 将查询索引转换为字符串以便分类绘图
df_success['query_index_str'] = df_success['query_index'].astype(str)
# 将总时间转换为毫秒以便阅读
df_success['total_time_ms'] = df_success['total_time_seconds'] * 1000

# --- 绘图 ---
# 设置绘图风格
sns.set_theme(style="whitegrid")
# 创建一个 Figure，包含多个子图 (Axes)
# 如果 operator_timings 数据有效，我们会需要更多空间
# 暂时创建 1x2 的布局，后续根据数据调整
fig, axes = plt.subplots(1, 2, figsize=(18, 6)) # 初始布局，后续可能调整
fig.suptitle('Query Performance Analysis', fontsize=16)

# --- 图 1: 各查询总执行时间 ---
ax1 = axes[0]
sns.barplot(x='query_index_str', y='total_time_ms', data=df_success, ax=ax1, palette='viridis', estimator=sum) # 使用 sum 以防有重复 index
ax1.set_title('Total Execution Time per Query')
ax1.set_xlabel('Query Index')
ax1.set_ylabel('Total Duration (ms)')
ax1.tick_params(axis='x', rotation=0)

# --- 图 2: 操作符耗时分析 (如果数据可用) ---
ax2 = axes[1] # 第二个子图用于操作符分析
operator_data = []
valid_operator_data_found = False

for index, row in df_success.iterrows():
    query_idx = row['query_index']
    timings = row.get('operator_timings', [])
    if isinstance(timings, list) and timings:
        # 检查是否包含错误信息而不是实际数据
        if not any('error' in op for op in timings):
            valid_operator_data_found = True
            for op in timings:
                # 确保 duration_ms 存在且为数字
                if 'operator_name' in op and 'duration_ms' in op and isinstance(op['duration_ms'], (int, float)):
                     operator_data.append({
                         'query_index': query_idx,
                         'operator_name': op['operator_name'],
                         'duration_ms': op['duration_ms']
                     })
                else:
                     print(f"Warning: Invalid operator data found in query {query_idx}: {op}")


if valid_operator_data_found and operator_data:
    df_operators = pd.DataFrame(operator_data)

    # --- 2a: 每个查询的操作符耗时堆叠图 (可能很拥挤) ---
    # 我们改为绘制 操作符耗时分布箱形图，更具洞察力

    # --- 2b: 操作符耗时分布 (箱形图) ---
    # 按操作符类型聚合，显示其耗时分布
    sns.boxplot(x='duration_ms', y='operator_name', data=df_operators, ax=ax2, palette='Spectral')
    ax2.set_title('Operator Duration Distribution (All Queries)')
    ax2.set_xlabel('Operator Duration (ms)')
    ax2.set_ylabel('Operator Name')
    # 可以考虑对 X 轴使用对数刻度，如果耗时差异很大
    # ax2.set_xscale('log')
    # ax2.set_xlabel('Operator Duration (ms, log scale)')
else:
    # 如果没有有效的 operator_timings 数据，显示提示信息
    ax2.text(0.5, 0.5, 'No valid operator timing data found\nPlease check logs and profiling setup',
             horizontalalignment='center', verticalalignment='center',
             fontsize=12, color='red', transform=ax2.transAxes)
    ax2.set_title('Operator Duration Distribution (No Data)')
    ax2.set_xticks([])
    ax2.set_yticks([])


# --- 调整布局并保存 ---
plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # 调整布局防止标题重叠
plot_path = os.path.join(output_dir, plot_filename)
try:
    plt.savefig(plot_path)
    print(f"Plot saved to: {plot_path}")
except Exception as e:
    print(f"Error saving plot: {e}")

plt.show() # 在屏幕上显示绘图 (如果环境支持)