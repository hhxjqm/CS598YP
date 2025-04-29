import json
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

# --- Paths ---
log_dir = 'log'
output_plot_dir = 'plots'
os.makedirs(output_plot_dir, exist_ok=True)

# --- Load logs ---
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

# --- Merge all ---
if not all_data:
    print("No data found.")
    exit()

full_df = pd.concat(all_data, ignore_index=True)

# --- Calculate average ingestion rate per experiment ---
avg_rates = (
    full_df
    .groupby(['memory', 'threads'])
    .agg(avg_ingestion_rate=('ingestion_rate_rows_per_sec', 'mean'))
    .reset_index()
)

# --- Build speedup table ---
speedup_data = []

for mem in avg_rates['memory'].unique():
    baseline_rate = avg_rates.query(f"memory == '{mem}' and threads == 1")['avg_ingestion_rate'].values[0]
    for threads in [1, 2, 4]:
        current_rate = avg_rates.query(f"memory == '{mem}' and threads == {threads}")['avg_ingestion_rate'].values[0]
        speedup = current_rate / baseline_rate
        speedup_data.append({
            'memory': mem,
            'threads': threads,
            'speedup': speedup
        })

speedup_df = pd.DataFrame(speedup_data)

# --- Plot Speedup Curves ---

plt.figure(figsize=(12,8))

for mem, group_df in speedup_df.groupby('memory'):
    plt.plot(
        group_df['threads'],
        group_df['speedup'],
        marker='o',
        label=f'{mem.upper()}'
    )

# Ideal speedup line (perfect linear scaling)
ideal_threads = [1, 2, 4]
ideal_speedup = [1, 2, 4]
plt.plot(ideal_threads, ideal_speedup, linestyle='--', color='gray', label='Ideal Speedup')

plt.xticks([1, 2, 4])
plt.xlabel('Number of Threads')
plt.ylabel('Speedup vs 1 Thread')
plt.title('DuckDB Ingestion Speedup vs Threads (Different Memory Limits)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()

plot_path = os.path.join(output_plot_dir, 'speedup_curve.png')
plt.savefig(plot_path)
print(f"✅ Saved speedup plot to {plot_path}")

# Optional: show the plot immediately
# plt.show()
