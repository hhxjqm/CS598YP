import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates # 用于处理时间轴，如果需要的话
import os # 导入 os 模块用于创建目录

# --- 配置参数 ---
# 日志文件路径 (与你的脚本一致)
log_file = 'log/ingestion_log_2cpu_2ram.jsonl' # 确保这个路径是正确的

# 图表保存路径和文件名
output_plot_path = 'plots/ingestion_metrics_2cpu_2ram.png' # 示例：保存到 plots 目录下的 png 文件

# --- 确保图表输出目录存在 ---
output_dir = os.path.dirname(output_plot_path)
if output_dir and not os.path.exists(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print(f"确保图表输出目录存在: {output_dir}")


# --- 读取和解析日志文件 ---
log_data = []

try:
    print(f"正在读取日志文件: {log_file}")
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                log_entry = json.loads(line)
                # 只处理成功的插入记录
                if log_entry.get('status') == 'SUCCESS':
                    log_data.append(log_entry)
            except json.JSONDecodeError as e:
                print(f"跳过无效的 JSON 行 (解析错误: {e}): {line.strip()}")
            except KeyError as e:
                 print(f"跳过缺少关键信息 ({e}) 的行: {line.strip()}")

except FileNotFoundError:
    print(f"错误: 未找到日志文件在 {log_file}")
    exit() # 如果日志文件不存在，程序无法继续
except Exception as e:
    print(f"读取日志文件时发生错误: {e}")
    exit() # 如果读取文件出错，程序无法继续

# --- 准备数据进行绘图 ---
if not log_data:
    print("日志文件中没有找到成功的插入数据。请确保导入脚本已成功运行并生成了日志。")
else:
    # 将解析后的数据转换为 pandas DataFrame，方便操作
    df = pd.DataFrame(log_data)

    # --- 数据处理 ---
    # 将 timestamp 字符串转换为 datetime 对象 (如果需要用时间作为X轴)
    # df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 计算每个块的磁盘写入速率 (MB/秒)
    # disk_io_delta_during_chunk_bytes['write'] 是该块处理期间的总写入字节数
    # time_taken_seconds 是该块处理所花费的时间
    # 避免除以零的情况
    df['disk_write_mb_per_sec'] = (df['disk_io_delta_during_chunk_bytes'].apply(lambda x: x.get('write', 0)) / (1024 * 1024)) / df['time_taken_seconds'].replace(0, pd.NA).fillna(0.0001)
    df['disk_read_mb_per_sec'] = (df['disk_io_delta_during_chunk_bytes'].apply(lambda x: x.get('read', 0)) / (1024 * 1024)) / df['time_taken_seconds'].replace(0, pd.NA).fillna(0.0001)

    # 确保 chunk_index 是数字类型，并按升序排列 (通常日志已经是按顺序的)
    df['chunk_index'] = df['chunk_index'].astype(int)
    df = df.sort_values('chunk_index')

    # --- 绘图 ---
    print("正在生成图表...")

    # 创建一个图形和两个子图，共享X轴
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(15, 10), sharex=True)

    # 子图 1: 插入速率 vs. 块编号
    axes[0].plot(df['chunk_index'], df['ingestion_rate_rows_per_sec'], label='Ingestion Rate (rows/sec)', color='blue')
    axes[0].set_ylabel('Ingestion Rate (rows/sec)', color='blue')
    axes[0].tick_params(axis='y', labelcolor='blue')
    axes[0].set_title('DuckDB Ingestion Rate and System Metrics by Chunk Index')
    axes[0].grid(True, linestyle='--', alpha=0.6)
    axes[0].legend(loc='upper left')


    # 子图 2: 系统指标 vs. 块编号
    # 绘制 CPU 和内存使用率
    axes[1].plot(df['chunk_index'], df['system_metrics_after_chunk'].apply(lambda x: x.get('cpu_percent', -1)), label='CPU Util (%)', color='orange')
    axes[1].plot(df['chunk_index'], df['system_metrics_after_chunk'].apply(lambda x: x.get('memory_percent', -1)), label='Memory Util (%)', color='red')
    axes[1].set_ylabel('Utilization (%)', color='black') # 使用黑色作为默认文字颜色
    axes[1].tick_params(axis='y', labelcolor='black')


    # 为磁盘 I/O 创建次级 Y 轴
    ax2 = axes[1].twinx()
    ax2.plot(df['chunk_index'], df['disk_write_mb_per_sec'], label='Disk Write (MB/sec)', color='green', linestyle='--')
    # 如果需要，也可以绘制读取速率
    # ax2.plot(df['chunk_index'], df['disk_read_mb_per_sec'], label='Disk Read (MB/sec)', color='purple', linestyle=':')

    ax2.set_ylabel('Disk I/O Rate (MB/sec)', color='green')
    ax2.tick_params(axis='y', labelcolor='green')

    # 合并两个Y轴的图例
    lines, labels = axes[1].get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    axes[1].legend(lines + lines2, labels + labels2, loc='upper right')

    axes[1].set_xlabel('Chunk Index')
    axes[1].grid(True, linestyle='--', alpha=0.6)


    # 如果想用时间作为X轴，可以取消注释下面的代码，并注释掉上面的 chunk_index 相关设置
    # axes[1].set_xlabel('Time')
    # axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    # axes[1].xaxis.set_major_locator(mdates.AutoDateLocator())
    # fig.autofmt_xdate() # 自动格式化日期标签以避免重叠


    plt.tight_layout() # 自动调整子图布局以防止重叠

    # --- 将图表保存到文件 ---
    # 定义保存文件的路径和名称
    # output_plot_path = 'ingestion_metrics.png' # 已经移到顶部配置区
    plt.savefig(output_plot_path, bbox_inches='tight') # bbox_inches='tight' 防止标签被截断

    # 打印保存成功的消息
    print(f"图表已保存到: {output_plot_path}")

    # 如果你仍然希望在保存后显示图表（可选，通常保存后就不需要弹窗了）
    # plt.show()


    print("图表生成脚本执行完毕。")