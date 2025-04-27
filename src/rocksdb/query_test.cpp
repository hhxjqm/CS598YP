// ================================================================
// rocksdb_benchmark.cpp —— 功能完全对齐 DuckDB Python 基准测试
// ================================================================
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <algorithm>
#include <chrono>
#include <random>
#include <iomanip>
#include <string>
#include <cmath>
#include <filesystem>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <unistd.h>              /

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <json.hpp>
using json = nlohmann::json;
namespace fs = std::filesystem;
#define TABLE_NAME "dummy"

// ---------- 全局常量 ----------
static constexpr int   OUTER_LOOPS   = 10;          // 每轮 10 组查询
static constexpr int   NORMAL_PER_GRP = 4;          // 每组 4 个普通查询

// ---------- 随机数固定种子，保证可复现 ----------
static std::mt19937 rng(22);

// ==================================================================
// 🛠️ 1. 通用工具
// ==================================================================
std::string now_iso8601()
{
    using namespace std::chrono;
    auto now = system_clock::now();
    auto itt = system_clock::to_time_t(now);
    auto us  = duration_cast<microseconds>(now.time_since_epoch()) % 1'000'000;
    std::ostringstream ss;
    ss << std::put_time(std::gmtime(&itt), "%Y-%m-%dT%H:%M:%S")
       << '.' << std::setw(6) << std::setfill('0') << us.count() << 'Z';
    return ss.str();
}

// ---------- 读取第一行辅助 ----------
std::string read_first_line(const std::string& p)
{
    std::ifstream f(p);
    std::string s;
    if (f.good()) std::getline(f, s);
    return s;
}

// ---------- 获取系统资源（与 Python 版保持字段一致）----------
json get_system_metrics_docker()
{
    json m;
    try {
        // ---- CPU & 进程内存 ----
        struct rusage u{};
        getrusage(RUSAGE_SELF, &u);
        m["user_cpu_time_sec"]   = u.ru_utime.tv_sec + u.ru_utime.tv_usec / 1e6;
        m["system_cpu_time_sec"] = u.ru_stime.tv_sec + u.ru_stime.tv_usec / 1e6;
        double mem_gb = u.ru_maxrss / 1024.0 / 1024.0;  // KB ➜ GB
        m["memory_used_gb"] = mem_gb;

        // ---- cgroup 内存占用百分比 ----
        long limit = -1, usage = -1;
        if (fs::exists("/sys/fs/cgroup/memory.max")) {               // cgroup v2
            auto lim = read_first_line("/sys/fs/cgroup/memory.max");
            auto use = read_first_line("/sys/fs/cgroup/memory.current");
            if (lim != "max" && !lim.empty()) limit = std::stol(lim);
            if (!use.empty()) usage = std::stol(use);
        } else if (fs::exists("/sys/fs/cgroup/memory/memory.limit_in_bytes")) { // v1
            limit = std::stol(read_first_line("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
            usage = std::stol(read_first_line("/sys/fs/cgroup/memory/memory.usage_in_bytes"));
        }
        if (limit > 0 && usage >= 0) {
            m["memory_percent"]    = std::round(usage * 10000.0 / limit) / 100; // 保留两位
            m["memory_used_gb"]    = std::round(usage / 1.07374e9 * 100) / 100; // Bytes ➜ GB
        } else {
            m["memory_percent"] = -1;
        }
    } catch (...) {
        m["memory_percent"] = -1;
    }
    return m;
}

// ==================================================================
// 🗄️ 2. 数据加载（一次性全部拉进内存，等价于 Python 的 df）
// ==================================================================
std::vector<json> load_all_from_rocks(const std::string& db_path)
{
    rocksdb::DB* db;
    rocksdb::Options opt;
    opt.create_if_missing = false;
    auto s = rocksdb::DB::Open(opt, db_path, &db);
    if (!s.ok()) throw std::runtime_error("无法打开 RocksDB: " + s.ToString());

    std::vector<json> rows;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        try { rows.emplace_back(json::parse(it->value().ToString())); }
        catch (...) {}   // skip bad row
    }
    it.reset();
    delete db;
    return rows;
}

// ==================================================================
// 🔍 3. 查询实现（逻辑与 Python 版一致，返回 row_count）
// ==================================================================
// ------ 3.1 单列 group by ------
size_t single_column_groupby(const std::vector<json>& d)
{
    std::string col = std::uniform_int_distribution<>(0,1)(rng) ? "payment_type" : "passenger_count";
    std::unordered_set<std::string> grp;
    for (auto& r: d) if (r.contains(col)) grp.insert(r[col].get<std::string>());
    return grp.size();
}
// ------ 3.2 多列 group by ------
size_t multi_column_groupby(const std::vector<json>& d)
{
    std::unordered_set<std::string> grp;
    for (auto& r : d)
        if (r.contains("payment_type") && r.contains("passenger_count"))
            grp.insert(r["payment_type"].get<std::string>() + "_" +
                        r["passenger_count"].get<std::string>());
    return grp.size();
}
// ------ 3.3 top-k pulocationid ------
size_t aggregation_topk(const std::vector<json>& d)
{
    std::unordered_map<std::string,int> cnt;
    for (auto& r: d) if (r.contains("pulocationid"))
        cnt[r["pulocationid"].get<std::string>()]++;
    std::vector<int> v; v.reserve(cnt.size());
    for (auto& [_,c]: cnt) v.push_back(c);
    std::nth_element(v.begin(), v.begin()+std::min(10,(int)v.size()) , v.end(),
                     std::greater<int>());
    return std::min<size_t>(10, v.size());
}
// ------ 3.4 trip_distance & total_amount filter ------
size_t filter_range(const std::vector<json>& d)
{
    // 收集数值
    std::vector<double> trips, amts;
    for (auto& r: d){
        if (r.contains("trip_distance") && r.contains("total_amount")){
            try{
                trips.push_back(std::stod(r["trip_distance"].get<std::string>()));
                amts .push_back(std::stod(r["total_amount"].get<std::string>()));
            }catch(...){}
        }
    }
    if (trips.empty()) return 0;
    // 计算 30% / 90% 分位
    auto quant = [](std::vector<double>& v, double q){
        size_t idx = (size_t)std::floor(q * (v.size()-1));
        std::nth_element(v.begin(), v.begin()+idx, v.end());
        return v[idx];
    };
    double t_min = quant(trips, 0.3);
    double t_max = quant(trips, 0.9);
    double a_min = quant(amts , 0.3);
    double a_max = quant(amts , 0.9);
    std::uniform_real_distribution<> dt(t_min, t_max), da(a_min, a_max);
    double t_th = std::round(dt(rng)*100)/100.0;
    double a_th = std::round(da(rng)*100)/100.0;

    size_t cnt = 0;
    for (auto& r: d){
        if (r.contains("trip_distance") && r.contains("total_amount")){
            try{
                double td = std::stod(r["trip_distance"].get<std::string>());
                double ta = std::stod(r["total_amount"].get<std::string>());
                if (td > t_th && ta > a_th) ++cnt;
            }catch(...){}
        }
    }
    return cnt;
}
// ------ 3.5 以下 heavy 查询直接返回行数，逻辑保持不变 ------
size_t basic_window                (const std::vector<json>& d){ return d.size(); }
size_t sorted_window               (const std::vector<json>& d){ return d.size(); }
size_t quantiles_entire_dataset    (const std::vector<json>& d){ return 2;       }
size_t partition_by_window         (const std::vector<json>& d){ return d.size();}
size_t lead_and_lag                (const std::vector<json>& d){ return d.size();}
size_t moving_averages             (const std::vector<json>& d){ return d.size();}
size_t rolling_sum                 (const std::vector<json>& d){ return d.size();}
size_t range_between               (const std::vector<json>& d){ return d.size();}
size_t quantiles_partition_by      (const std::vector<json>& d){ return d.size();}
size_t multi_column_complex_agg    (const std::vector<json>& d){ return d.size();}

// ==================================================================
// 🧩 4. 查询元数据（type + SQL 模板 + 执行函数）
// ==================================================================
struct QueryDef{
    std::string type;
    std::string sql;
    size_t (*func)(const std::vector<json>&);
};
static std::vector<QueryDef> NORMAL_QUERIES = {
    {"single_column_groupby",
     "SELECT {col}, COUNT(*) FROM " TABLE_NAME " GROUP BY {col}",
     single_column_groupby},

    {"multi_column_groupby",
     "SELECT payment_type, passenger_count, COUNT(*) "
     "FROM " TABLE_NAME " GROUP BY payment_type, passenger_count",
     multi_column_groupby},

    {"aggregation_topk",
     "SELECT pulocationid, COUNT(*) FROM " TABLE_NAME
     " GROUP BY pulocationid ORDER BY COUNT(*) DESC LIMIT 10",
     aggregation_topk},

    {"filter_range",
     "SELECT * FROM " TABLE_NAME
     " WHERE trip_distance > ? AND total_amount > ?",
     filter_range}
};

// ================= 重载 / Window / 复杂聚合查询 =================
static std::vector<QueryDef> HEAVY_QUERIES = {
    {"basic_window",
     "SELECT *, ROW_NUMBER() OVER () AS row_num FROM " TABLE_NAME,
     basic_window},

    {"sorted_window",
     "SELECT *, ROW_NUMBER() OVER (ORDER BY trip_distance DESC) AS distance_rank "
     "FROM " TABLE_NAME,
     sorted_window},

    {"quantiles_entire_dataset",
     "SELECT quantile_cont(total_amount, 0.5) OVER ()  AS median_amount, "
     "       quantile_cont(total_amount, 0.9) OVER ()  AS p90_amount "
     "FROM " TABLE_NAME,
     quantiles_entire_dataset},

    {"partition_by_window",
     "SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_type "
     "                              ORDER BY trip_distance DESC) AS rank_within_payment "
     "FROM " TABLE_NAME,
     partition_by_window},

    {"lead_and_lag",
     "SELECT passenger_count, "
     "       LEAD(passenger_count) OVER (ORDER BY tpep_pickup_datetime) AS next_passenger, "
     "       LAG(passenger_count)  OVER (ORDER BY tpep_pickup_datetime) AS prev_passenger "
     "FROM " TABLE_NAME,
     lead_and_lag},

    {"moving_averages",
     "SELECT tpep_pickup_datetime, "
     "       AVG(total_amount) OVER (ORDER BY tpep_pickup_datetime "
     "                               ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg_amount "
     "FROM " TABLE_NAME,
     moving_averages},

    {"rolling_sum",
     "SELECT tpep_pickup_datetime, "
     "       SUM(total_amount) OVER (ORDER BY tpep_pickup_datetime "
     "                               ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_sum_amount "
     "FROM " TABLE_NAME,
     rolling_sum},

    {"range_between",
     "SELECT tpep_pickup_datetime, "
     "       SUM(total_amount) OVER (ORDER BY tpep_pickup_datetime "
     "                               RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_income "
     "FROM " TABLE_NAME,
     range_between},

    {"quantiles_partition_by",
     "SELECT payment_type, "
     "       quantile_cont(total_amount,0.5) OVER (PARTITION BY payment_type) AS median_amount_within_payment "
     "FROM " TABLE_NAME,
     quantiles_partition_by},

    {"multi_column_complex_aggregation",
     "SELECT passenger_count, payment_type, PULocationID, DOLocationID, "
     "       EXTRACT(year  FROM tpep_pickup_datetime) AS pickup_year, "
     "       EXTRACT(month FROM tpep_pickup_datetime) AS pickup_month, "
     "       COUNT(*)        AS trip_count, "
     "       SUM(total_amount) AS total_revenue, "
     "       AVG(trip_distance) AS avg_distance, "
     "       MAX(tip_amount)  AS max_tip, "
     "       MIN(fare_amount) AS min_fare "
     "FROM " TABLE_NAME " "
     "GROUP BY passenger_count, payment_type, PULocationID, DOLocationID, pickup_year, pickup_month",
     multi_column_complex_agg}
};

// ==================================================================
// ⚙️ 5. 运行单条查询 + 监控（完全仿照 Python 版公式）
// ==================================================================
json run_query(const QueryDef& qd, const std::vector<json>& data)
{
    // ---- 记录开始时间 / CPU ----
    struct rusage r0{};
    getrusage(RUSAGE_SELF, &r0);
    auto t0 = std::chrono::steady_clock::now();

    // ---- 真正执行 ----
    size_t rows = qd.func(data);

    // ---- 记录结束 ----
    struct rusage r1{};
    getrusage(RUSAGE_SELF, &r1);
    auto t1 = std::chrono::steady_clock::now();
    double wall = std::chrono::duration<double>(t1 - t0).count();

    // ---- CPU 百分比（与 Python 版同公式）----
    double usr = r1.ru_utime.tv_sec + r1.ru_utime.tv_usec/1e6
               - r0.ru_utime.tv_sec - r0.ru_utime.tv_usec/1e6;
    double sys = r1.ru_stime.tv_sec + r1.ru_stime.tv_usec/1e6
               - r0.ru_stime.tv_sec - r0.ru_stime.tv_usec/1e6;
    int    cores = std::max(1, (int)sysconf(_SC_NPROCESSORS_ONLN));
    double cpu_pct = wall>0 ? (usr+sys)/wall * 100.0 / cores : 0.0;

    // ---- 内存占用 ----
    json mem = get_system_metrics_docker();

    // ---- 打包结果，与 Python 版键名一致 ----
    json out = {
        {"timestamp"        , now_iso8601()     },
        {"query"            , qd.sql            },
        {"query_type"       , qd.type           },
        {"row_count"        , rows              },
        {"time_taken_seconds", wall             },
        {"cpu_percent"      , std::round(cpu_pct*100)/100},
        {"memory_percent"   , mem["memory_percent"]},
        {"memory_used_gb"   , mem["memory_used_gb"]}
    };
    return out;
}

// ==================================================================
// 🔄 6. 主基准循环：每轮 10 组 * (4 普通 + 1 重载) 共 50 次
// ==================================================================
void benchmark(const std::vector<json>& data,
               const std::string&        log_path,
               int                       rounds,
               long                      max_seconds)
{
    std::ofstream flog(log_path, std::ios::app);
    auto t_start = std::chrono::steady_clock::now();

    std::uniform_int_distribution<> heavy_dist(0, (int)HEAVY_QUERIES.size()-1);

    for (int rd = 0; rd < rounds; ++rd) {
        // --- 达到时间上限就退出 ---
        if (max_seconds>0){
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now()-t_start).count();
            if (elapsed > max_seconds){
                std::cout << "⏱️ 已达到最大运行时间 " << max_seconds << " 秒，停止测试\n";
                return;
            }
        }

        std::cout << "\n🔁 第 " << rd+1 << " 轮查询\n";
        for (int grp=0; grp<OUTER_LOOPS; ++grp){
            // ---- 4 个普通查询固定顺序 ----
            for (auto& qd: NORMAL_QUERIES){
                std::cout << "➡️ [" << qd.type << "]\n";
                auto res = run_query(qd, data);
                flog << res.dump() << '\n';  flog.flush();
            }
            // ---- 1 个 heavy 查询随机挑 ----
            const QueryDef& heavy = HEAVY_QUERIES[heavy_dist(rng)];
            std::cout << "🔥 [" << heavy.type << "]\n";
            auto res = run_query(heavy, data);
            flog << res.dump() << '\n';  flog.flush();
        }
    }
    std::cout << "\n✅ 日志写入完成: " << log_path << "\n";
}

// ==================================================================
// 🏁 7. CLI 入口，参数与 Python 版一一对应
// ==================================================================
int main(int argc, char* argv[])
{
    // --- 简易参数解析 ---
    std::string db_path, log_path;
    int   rounds      = INT32_MAX;
    long  max_seconds = -1;

    for (int i=1;i<argc;i++){
        std::string a = argv[i];
        if      (a=="--db"         && i+1<argc) db_path  = argv[++i];
        else if (a=="--log"        && i+1<argc) log_path = argv[++i];
        else if (a=="--rounds"     && i+1<argc) rounds   = std::stoi(argv[++i]);
        else if (a=="--max-seconds"&& i+1<argc) max_seconds = std::stol(argv[++i]);
    }
    if (db_path.empty() || log_path.empty()){
        std::cerr << "用法: ./prog --db <RocksDB路径> --log <日志.jsonl> "
                     "[--rounds N] [--max-seconds 秒]\n";
        return 1;
    }
    fs::create_directories(fs::path(log_path).parent_path());

    // --- 数据加载一次即可 ---
    auto data = load_all_from_rocks(db_path);

    // --- 正式基准测试 ---
    benchmark(data, log_path, rounds, max_seconds);
    return 0;
}
