import json
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

# --- Paths ---
log_dir = 'log'
output_plot_dir = 'plots'
os.makedirs(output_plot_dir, exist_ok=True)

# --- Find all log files ---
log_files = glob.glob(os.path.join(log_dir, 'ingestion_log_*.jsonl'))
print(f"Found {len(log_files)} log files.")

# --- Prepare for plotting ---
all_data = []

for log_file in log_files:
    experiment_name = os.path.basename(log_file).replace('ingestion_log_', '').replace('.jsonl', '')
    memory_setting, thread_setting = experiment_name.split('_')
    
    log_entries = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            entry = json.loads(line)
            if entry.get('status') == 'SUCCESS':
                entry['experiment'] = experiment_name
                entry['memory'] = memory_setting
                entry['threads'] = int(thread_setting.replace('threads', ''))
                log_entries.append(entry)
    
    if log_entries:
        df = pd.DataFrame(log_entries)
        all_data.append(df)

# --- Merge all logs into one big DataFrame ---
if not all_data:
    print("No data found.")
    exit()

full_df = pd.concat(all_data, ignore_index=True)

# --- Now we can plot! ---
# (1) Ingestion Rate (rows/sec) comparison

plt.figure(figsize=(16,10))

for (mem, th), group_df in full_df.groupby(['memory', 'threads']):
    plt.plot(
        group_df['chunk_index'],
        group_df['ingestion_rate_rows_per_sec'],
        label=f"{mem.upper()}, {th} Threads"
    )

plt.xlabel('Chunk Index')
plt.ylabel('Ingestion Rate (Rows/sec)')
plt.title('DuckDB Ingestion Rate Comparison (Different Memory and Threads)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()

plot_path = os.path.join(output_plot_dir, 'ingestion_rate_comparison.png')
plt.savefig(plot_path)
print(f"Saved ingestion rate plot to {plot_path}")

# --- (2) CPU usage (%) comparison ---

plt.figure(figsize=(16,10))

for (mem, th), group_df in full_df.groupby(['memory', 'threads']):
    plt.plot(
        group_df['chunk_index'],
        group_df['system_metrics_after_chunk'].apply(lambda x: x.get('cpu_percent', -1)),
        label=f"{mem.upper()}, {th} Threads"
    )

plt.xlabel('Chunk Index')
plt.ylabel('CPU Usage (%)')
plt.title('CPU Usage During Ingestion (Different Memory and Threads)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()

plot_path = os.path.join(output_plot_dir, 'cpu_usage_comparison.png')
plt.savefig(plot_path)
print(f"Saved CPU usage plot to {plot_path}")

# --- (3) Memory Usage (GB) comparison ---

plt.figure(figsize=(16,10))

for (mem, th), group_df in full_df.groupby(['memory', 'threads']):
    plt.plot(
        group_df['chunk_index'],
        group_df['system_metrics_after_chunk'].apply(lambda x: x.get('memory_used_gb', -1)),
        label=f"{mem.upper()}, {th} Threads"
    )

plt.xlabel('Chunk Index')
plt.ylabel('Memory Used (GB)')
plt.title('Memory Usage During Ingestion (Different Memory and Threads)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()

plot_path = os.path.join(output_plot_dir, 'memory_usage_comparison.png')
plt.savefig(plot_path)
print(f"Saved memory usage plot to {plot_path}")

print("✅ All plots generated successfully!")
