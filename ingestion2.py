import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import glob

# --- Config ---
log_base_dir = 'log'     # where all your log files are
output_plot_dir = 'plots'  # where to save the plots

os.makedirs(output_plot_dir, exist_ok=True)

# --- Load all logs ---
log_files = glob.glob(os.path.join(log_base_dir, 'ingestion_log_*.jsonl'))

experiment_results = []

for log_file in log_files:
    experiment_name = os.path.basename(log_file).replace('ingestion_log_', '').replace('.jsonl', '')

    rows_ingested = 0
    total_time = 0.0001  # avoid division by zero

    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            log_entry = json.loads(line)
            if log_entry.get('status') == 'SUCCESS':
                rows_ingested += log_entry.get('rows_ingested', 0)
                total_time += log_entry.get('time_taken_seconds', 0)

    avg_rate = rows_ingested / total_time
    memory, threads = experiment_name.split('_')
    threads = threads.replace('threads', '')

    experiment_results.append({
        'experiment': experiment_name,
        'memory_limit': memory,
        'threads': int(threads),
        'rows_ingested': rows_ingested,
        'total_time': total_time,
        'avg_ingestion_rate': avg_rate
    })

# --- Convert to DataFrame ---
df = pd.DataFrame(experiment_results)
print(df)

# --- Plot average ingestion rate ---
plt.figure(figsize=(14, 8))

for memory_limit in df['memory_limit'].unique():
    subset = df[df['memory_limit'] == memory_limit]
    subset = subset.sort_values('threads')
    plt.plot(subset['threads'], subset['avg_ingestion_rate'], marker='o', label=f'Memory {memory_limit}')

plt.title('DuckDB Ingestion Rate Comparison by Memory and Threads')
plt.xlabel('Number of Threads')
plt.ylabel('Average Ingestion Rate (rows/sec)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(title='Memory Limit')
plt.xticks(df['threads'].unique())
plt.tight_layout()

# --- Save figure ---
output_path = os.path.join(output_plot_dir, 'ingestion_rate_comparison.png')
plt.savefig(output_path)
print(f"Plot saved to: {output_path}")

# Optionally show the plot
# plt.show()
