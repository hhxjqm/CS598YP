import json
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import numpy as np

# --- Paths ---
log_dir = 'log'
output_plot_dir = 'plots'
os.makedirs(output_plot_dir, exist_ok=True)

# --- Load all log files ---
log_files = glob.glob(os.path.join(log_dir, 'ingestion_log_*.jsonl'))
print(f"Found {len(log_files)} log files.")

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

# --- Merge all logs ---
if not all_data:
    print("No data found.")
    exit()

full_df = pd.concat(all_data, ignore_index=True)

# --- Group chunks into bins of 500 ---
bin_size = 500
full_df['chunk_bin'] = (full_df['chunk_index'] // bin_size) * bin_size  # group into 0-499, 500-999, etc.

# --- Calculate average ingestion rate per bin ---
avg_df = (
    full_df
    .groupby(['memory', 'threads', 'chunk_bin'])
    .agg(avg_ingestion_rate=('ingestion_rate_rows_per_sec', 'mean'))
    .reset_index()
)

# --- Plot Line Plot ---

plt.figure(figsize=(16,10))

for (mem, th), group_df in avg_df.groupby(['memory', 'threads']):
    label = f"{mem.upper()}, {th} Threads"
    plt.plot(
        group_df['chunk_bin'],
        group_df['avg_ingestion_rate'],
        marker='o',
        label=label
    )

plt.xlabel('Chunk Index (grouped by 500)')
plt.ylabel('Average Ingestion Rate (Rows/sec)')
plt.title('DuckDB Ingestion Rate Averaged Over 500 Chunks')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()

plot_path = os.path.join(output_plot_dir, 'ingestion_rate_avg_500chunks.png')
plt.savefig(plot_path)
print(f"✅ Saved plot to {plot_path}")

# Optional: show the plot immediately
# plt.show()
