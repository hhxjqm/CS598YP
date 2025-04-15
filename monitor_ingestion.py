import duckdb, time, numpy as np, psutil, os, socket, csv
from datetime import datetime  # ✅ 用于生成合法的 TIMESTAMP 类型

# 参数配置
DB_FILE = "test.duckdb"
CSV_LOG = "ingestion_log.csv"
BATCH_SIZE = 10000
NUM_BATCHES = 500

# 初始化
con = duckdb.connect(DB_FILE)
con.execute("CREATE TABLE IF NOT EXISTS data (id BIGINT, ts TIMESTAMP, value DOUBLE);")

proc = psutil.Process(os.getpid())
hostname = socket.gethostname()

# 准备 CSV 文件
with open(CSV_LOG, mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["hostname", "batch", "inserted_rows", "time_sec", "rss_mb", "cpu_percent"])

    for i in range(NUM_BATCHES):
        t0 = time.time()
        now = datetime.now()  # ✅ 合法的 TIMESTAMP 值
        data = [(j + i * BATCH_SIZE, now, np.random.rand()) for j in range(BATCH_SIZE)]
        con.executemany("INSERT INTO data VALUES (?, ?, ?)", data)
        elapsed = time.time() - t0

        # 记录资源信息
        rss = proc.memory_info().rss / 1024 / 1024  # MB
        cpu = proc.cpu_percent(interval=None)
        writer.writerow([hostname, i+1, BATCH_SIZE * (i+1), round(elapsed, 4), round(rss, 2), round(cpu, 2)])

        print(f"[{hostname}] Batch {i+1} inserted ({BATCH_SIZE} rows) in {elapsed:.3f}s | RSS={rss:.1f}MB | CPU={cpu:.1f}%")
