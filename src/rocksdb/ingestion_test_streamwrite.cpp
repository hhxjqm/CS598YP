#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <filesystem>
#include <string>
#include <iomanip>
#include <json.hpp>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statvfs.h> // ⭐ 补上这个
#include <unistd.h>
#include <cstdlib>
#include <ctime>

using json = nlohmann::json;
namespace fs = std::filesystem;
static std::mt19937 rng(22);


// 在 load_csv 之后，get_system_metrics_docker 之前，插入这个函数：
/**
 * 对单条 JSON 数据进行随机扰动 (perturbation)：
 * - trip_distance ±20%
 * - total_amount ±5
 * - passenger_count 随机 1–4
 * - tip_amount 随机 0–5
 * - tpep_pickup_datetime +1~30 分钟
 * - tpep_dropoff_datetime +5~20 分钟
 */
void perturb_row(json &row) {
    // 1. trip_distance ±20%
    if (row.contains("trip_distance")) {
        try {
            double d = std::stod(row["trip_distance"].get<std::string>());
            std::uniform_real_distribution<> fd(0.8, 1.2);
            d = std::round(d * fd(rng) * 100.0) / 100.0;
            row["trip_distance"] = std::to_string(d);
        } catch(...) {}
    }
    // 2. total_amount ±5
    if (row.contains("total_amount")) {
        try {
            double a = std::stod(row["total_amount"].get<std::string>());
            std::uniform_real_distribution<> fa(-5.0, 5.0);
            a = std::round((a + fa(rng)) * 100.0) / 100.0;
            row["total_amount"] = std::to_string(a);
        } catch(...) {}
    }
    // 3. passenger_count 随机 1–4
    if (row.contains("passenger_count")) {
        std::uniform_int_distribution<> ip(1, 4);
        row["passenger_count"] = std::to_string(ip(rng));
    }
    // 4. tip_amount 随机 0–5
    if (row.contains("tip_amount")) {
        std::uniform_real_distribution<> ft(0.0, 5.0);
        double t = std::round(ft(rng)*100.0)/100.0;
        row["tip_amount"] = std::to_string(t);
    }
    // 准备时间偏移
    auto parse_dt = [&](const std::string &s)->std::chrono::system_clock::time_point {
        std::tm tm{}; std::istringstream ss(s);
        ss >> std::get_time(&tm, "%m/%d/%Y %I:%M:%S %p");
        return std::chrono::system_clock::from_time_t(std::mktime(&tm));
    };
    auto format_dt = [&](const std::chrono::system_clock::time_point &tp)->std::string {
        auto tt = std::chrono::system_clock::to_time_t(tp);
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                      tp.time_since_epoch()) % 1000000;
        std::ostringstream os;
        os << std::put_time(std::gmtime(&tt), "%m/%d/%Y %I:%M:%S %p")
           << '.' << std::setw(3) << std::setfill('0')
           << (us.count()/1000);
        return os.str();
    };
    // 5. tpep_pickup_datetime +1~30 分钟
    if (row.contains("tpep_pickup_datetime")) {
        try {
            auto tp = parse_dt(row["tpep_pickup_datetime"].get<std::string>());
            std::uniform_int_distribution<> fm(1,30);
            tp += std::chrono::minutes(fm(rng));
            row["tpep_pickup_datetime"] = format_dt(tp);
        } catch(...) {}
    }
    // 6. tpep_dropoff_datetime +5~20 分钟
    if (row.contains("tpep_dropoff_datetime")) {
        try {
            auto tp = parse_dt(row["tpep_dropoff_datetime"].get<std::string>());
            std::uniform_int_distribution<> fm(5,20);
            tp += std::chrono::minutes(fm(rng));
            row["tpep_dropoff_datetime"] = format_dt(tp);
        } catch(...) {}
    }
}



// --- 获取当前时间戳字符串，带微秒 ---
std::string get_current_timestamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto itt = system_clock::to_time_t(now);
    auto ms = duration_cast<microseconds>(now.time_since_epoch()) % 1000000;

    std::ostringstream ss;
    ss << std::put_time(std::gmtime(&itt), "%Y-%m-%dT%H:%M:%S");
    ss << '.' << std::setw(6) << std::setfill('0') << ms.count() << "Z";
    return ss.str();
}

// --- 读取 CSV 文件到 vector<json> ---
std::vector<json> load_csv(const std::string& filename, size_t max_rows = 5000) {
    std::vector<json> data;
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("无法打开CSV文件");
    }

    std::string line, header_line;
    std::getline(file, header_line);
    std::vector<std::string> headers;
    std::stringstream header_stream(header_line);
    std::string header;
    while (std::getline(header_stream, header, ',')) {
        std::transform(header.begin(), header.end(), header.begin(), ::tolower);
        headers.push_back(header);
    }

    size_t count = 0;
    while (std::getline(file, line) && count < max_rows) {
        json row;
        std::stringstream line_stream(line);
        std::string cell;
        size_t index = 0;
        while (std::getline(line_stream, cell, ',')) {
            if (index < headers.size()) {
                row[headers[index]] = cell;
            }
            index++;
        }
        data.push_back(row);
        count++;
    }

    return data;
}

// 读取文件第一行
std::string read_first_line(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return "";
    }
    std::string line;
    std::getline(file, line);
    return line;
}

// --- 获取系统资源指标 ---
json get_system_metrics_docker() {
    json metrics;

    try {
        struct rusage usage;
        getrusage(RUSAGE_SELF, &usage);

        double user_cpu_time = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6;
        double system_cpu_time = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
        metrics["user_cpu_time_sec"] = user_cpu_time;
        metrics["system_cpu_time_sec"] = system_cpu_time;

        long memory_used_kb = usage.ru_maxrss;
        double memory_used_gb = memory_used_kb / 1024.0 / 1024.0;
        metrics["memory_used_gb"] = memory_used_gb;

        long mem_limit = -1;
        long mem_usage = -1;

        if (fs::exists("/sys/fs/cgroup/memory.max")) {  // cgroup v2
            std::string mem_limit_str = read_first_line("/sys/fs/cgroup/memory.max");
            std::string mem_usage_str = read_first_line("/sys/fs/cgroup/memory.current");

            if (!mem_limit_str.empty() && mem_limit_str != "max") {
                mem_limit = std::stol(mem_limit_str);
            }
            if (!mem_usage_str.empty()) {
                mem_usage = std::stol(mem_usage_str);
            }
        } else if (fs::exists("/sys/fs/cgroup/memory/memory.limit_in_bytes")) {  // cgroup v1
            mem_limit = std::stol(read_first_line("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
            mem_usage = std::stol(read_first_line("/sys/fs/cgroup/memory/memory.usage_in_bytes"));
        }

        if (mem_limit > 0 && mem_usage >= 0) {
            double mem_percent = static_cast<double>(mem_usage) / mem_limit * 100.0;
            double mem_usage_gb = mem_usage / (1024.0 * 1024.0 * 1024.0);

            metrics["memory_percent"] = round(mem_percent * 100) / 100;
            metrics["memory_used_gb"] = round(mem_usage_gb * 100) / 100;
            metrics["memory_available_gb"] = round((mem_limit / (1024.0 * 1024.0 * 1024.0) - mem_usage_gb) * 100) / 100;
        } else {
            metrics["memory_percent"] = -1;
            metrics["memory_available_gb"] = -1;
        }

        struct statvfs fs_stats;
        if (statvfs("/", &fs_stats) == 0) {
            metrics["disk_io_counters"] = json{
                {"block_size", fs_stats.f_bsize},
                {"free_blocks", fs_stats.f_bfree},
                {"available_blocks", fs_stats.f_bavail},
                {"total_blocks", fs_stats.f_blocks}
            };
        } else {
            metrics["disk_io_counters"] = nullptr;
        }

    } catch (const std::exception& e) {
        std::cerr << "获取系统指标失败: " << e.what() << std::endl;
    }

    return metrics;
}

// --- 随机写入 RocksDB ---
void simulate_random_streaming(const std::string& csv_file,
                                const std::string& db_path,
                                const std::string& log_file,
                                size_t max_rows = 0,
                                size_t max_seconds = 0,
                                double delay_min = 0.1,
                                double delay_max = 1.0,
                                const std::string& mode = "random") {

    auto all_data = load_csv(csv_file);
    size_t row_count = all_data.size();
    std::vector<size_t> row_indices(row_count);
    for (size_t i = 0; i < row_count; ++i) row_indices[i] = i;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> rand_rows(1, 100);
    std::uniform_real_distribution<> rand_delay(delay_min, delay_max);
    std::uniform_int_distribution<> rand_index(0, row_count - 1);

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        throw std::runtime_error("无法打开RocksDB数据库: " + status.ToString());
    }

    std::ofstream log_f(log_file, std::ios::app);
    if (!log_f.is_open()) {
        throw std::runtime_error("无法打开日志文件");
    }

    auto start_time = std::chrono::steady_clock::now();
    size_t total_written = 0;

    while (true) {
        if (max_rows > 0 && total_written >= max_rows) {
            std::cout << "✅ 达到最大行数，退出。" << std::endl;
            break;
        }
        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (max_seconds > 0 && std::chrono::duration_cast<std::chrono::seconds>(elapsed).count() >= max_seconds) {
            std::cout << "✅ 达到最大运行时间，退出。" << std::endl;
            break;
        }

        size_t batch_size;
        double delay;

        if (mode == "fixed_rows") {
            batch_size = 10;
            delay = 1.0;
        } else if (mode == "scheduled_pattern") {
            int minutes_passed = std::chrono::duration_cast<std::chrono::minutes>(elapsed).count();
            batch_size = (minutes_passed % 12) + 1;
            delay = 1.0;
        } else {
            batch_size = rand_rows(gen);
            delay = rand_delay(gen);
        }

        rocksdb::WriteBatch batch;
        auto batch_start_time = std::chrono::steady_clock::now();
        json metrics_before = get_system_metrics_docker(); // 记录写入前资源使用

        try {
            //记的恢复
//            for (size_t i = 0; i < batch_size; ++i) {
//                size_t idx = rand_index(gen);
//                auto& row = all_data[idx];
//                std::string key = get_current_timestamp() + "_" + std::to_string(i);
//                std::string value = row.dump();
//                batch.Put(key, value);
//            }
            for (size_t i = 0; i < batch_size; ++i) {
                size_t idx = rand_index(gen);
                // 复制一份样本行，并做随机扰动
                json row = all_data[idx];
                perturb_row(row);
                // 生成唯一 key 和序列化后的 value
                std::string key   = get_current_timestamp() + "_" + std::to_string(i);
                std::string value = row.dump();
                batch.Put(key, value);
            }

            rocksdb::WriteOptions write_options;
            status = db->Write(write_options, &batch);
            if (!status.ok()) {
                throw std::runtime_error("批量写入失败: " + status.ToString());
            }

            auto batch_end_time = std::chrono::steady_clock::now();
            json metrics_after = get_system_metrics_docker(); // 记录写入后资源使用

            double wall_time_sec = std::chrono::duration<double>(batch_end_time - batch_start_time).count();

            // ⭐ 用差值计算CPU占用率
            double user_cpu_diff = metrics_after["user_cpu_time_sec"].get<double>() - metrics_before["user_cpu_time_sec"].get<double>();
            double system_cpu_diff = metrics_after["system_cpu_time_sec"].get<double>() - metrics_before["system_cpu_time_sec"].get<double>();
            double cpu_usage_percent = (user_cpu_diff + system_cpu_diff) / wall_time_sec * 100.0;

            json log_entry = {
                {"timestamp", get_current_timestamp()},
                {"status", "SUCCESS"},
                {"rows_ingested", batch_size},
                {"time_taken_seconds", wall_time_sec},
                {"ingestion_rate_rows_per_sec", batch_size / (wall_time_sec > 0 ? wall_time_sec : 1e-6)},
                {"cpu_percent", cpu_usage_percent},
                {"system_metrics", metrics_after}
            };
            log_f << log_entry.dump() << std::endl;
            log_f.flush();

            std::cout << "✅ 成功写入 " << batch_size << " 行，耗时 " << wall_time_sec << " 秒，CPU使用 " << cpu_usage_percent << "%\n";
            total_written += batch_size;

        } catch (const std::exception& e) {
            std::cerr << "❌ 写入异常: " << e.what() << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::duration<double>(delay));
    }

    delete db;
}

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "用法: ./program --csv CSV路径 --db RocksDB路径 --log 日志路径 [其他参数]" << std::endl;
        return 1;
    }

    std::string csv_file, db_path, log_file;
    size_t max_rows = 0;
    size_t max_seconds = 0;
    double delay_min = 0.1;
    double delay_max = 1.0;
    std::string mode = "random";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--csv") {
            csv_file = argv[++i];
        } else if (arg == "--db") {
            db_path = argv[++i];
        } else if (arg == "--log") {
            log_file = argv[++i];
        } else if (arg == "--max-rows") {
            max_rows = std::stoul(argv[++i]);
        } else if (arg == "--max-seconds") {
            max_seconds = std::stoul(argv[++i]);
        } else if (arg == "--delay-min") {
            delay_min = std::stod(argv[++i]);
        } else if (arg == "--delay-max") {
            delay_max = std::stod(argv[++i]);
        } else if (arg == "--mode") {
            mode = argv[++i];
        }
    }

    fs::create_directories(fs::path(log_file).parent_path());
    fs::create_directories(fs::path(db_path).parent_path());

    simulate_random_streaming(csv_file, db_path, log_file, max_rows, max_seconds, delay_min, delay_max, mode);

    return 0;
}
