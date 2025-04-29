import matplotlib.pyplot as plt
import pandas as pd
import os

# --- Load your summarized ingestion results ---
# (assuming you have a summary DataFrame already)
# If not, here’s how to build from a list manually:
summary_data = [
    # experiment, memory_limit, threads, rows_ingested, total_time, avg_ingestion_rate
    ('1g_1threads', '1g', 1, 38310226, 134.1362, 285606.912974),
    ('1g_2threads', '1g', 2, 38310226, 139.8079, 274020.466655),
    ('1g_4threads', '1g', 4, 38310226, 161.6928, 236932.170140),
    ('256m_1threads', '256m', 1, 38310226, 139.6568, 274316.939812),
    ('256m_2threads', '256m', 2, 38310226, 132.7075, 288681.694705),
    ('256m_4threads', '256m', 4, 38310226, 135.9623, 281768.874395),
    ('4g_1threads', '4g', 1, 38310226, 152.9385, 250494.322881),
    ('4g_2threads', '4g', 2, 38310226, 5551.8049, 6900.499331),  # suspicious data?
    ('4g_4threads', '4g', 4, 38310226, 148.8224, 257422.444471),
    ('512m_1threads', '512m', 1, 38310226, 139.2779, 275063.208162),
    ('512m_2threads', '512m', 2, 38310226, 132.5814, 289856.263850),
    ('512m_4threads', '512m', 4, 38310226, 136.6184, 280417.762176),
]

summary_df = pd.DataFrame(summary_data, columns=[
    'experiment', 'memory_limit', 'threads', 'rows_ingested', 'total_time', 'avg_ingestion_rate'
])

# --- Clean memory_limit labels ---
summary_df['memory_limit'] = summary_df['memory_limit'].str.lower()

# --- Prepare plot ---
plt.figure(figsize=(14, 8))

# Use distinct markers and color palette
markers = ['o', 's', '^', 'D', 'v', '*', 'X', 'P']
colors = plt.cm.tab10.colors  # 10 distinct colors
marker_map = {}

# Sort memory settings for consistent order
memory_order = sorted(summary_df['memory_limit'].unique())

for idx, mem in enumerate(memory_order):
    group_df = summary_df[summary_df['memory_limit'] == mem]
    marker = markers[idx % len(markers)]
    color = colors[idx % len(colors)]
    plt.plot(
        group_df['threads'],
        group_df['avg_ingestion_rate'],
        label=f'Memory {mem.upper()}',
        marker=marker,
        color=color,
        linewidth=2,
        markersize=8
    )

plt.xlabel('Number of Threads', fontsize=14)
plt.ylabel('Average Ingestion Rate (Rows/sec)', fontsize=14)
plt.title('DuckDB Ingestion Rate Comparison by Memory and Threads', fontsize=16)
plt.xticks([1, 2, 4], fontsize=12)
plt.yticks(fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=12, loc='best')
plt.tight_layout()

# --- Save plot ---
os.makedirs('plots', exist_ok=True)
plot_path = 'plots/ingestion_rate_comparison_improved.png'
plt.savefig(plot_path)
print(f"✅ Improved plot saved to {plot_path}")

# Optional: Display immediately
# plt.show()
