#include <iostream>
#include <string>
#include <vector>
#include <fstream> // 用于文件操作
#include <chrono>  // 用于计时
#include <iomanip> // 用于格式化输出和时间格式化
#include <limits>  // 用于读取 CSV 时的数值限制
#include <filesystem> // C++17 文件系统操作，用于创建目录
#include <ctime> // 用于时间转换
#include <sstream> // 用于时间格式化

// RocksDB 头文件
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/status.h"

// 第三方库头文件
#include "csv.h" // Fast-CPP-CSV-Parser
#include "json.hpp" // nlohmann/json

// --- 配置参数 ---
// CSV 数据文件路径
const std::string kCsvFile = "data_set/2023_Yellow_Taxi_Trip_Data.csv";
// RocksDB 数据库路径 (RocksDB 数据库是一个目录)
const std::string kDBPath = "db/taxi_rocksdb_cpp_csv"; // 修改为不同的目录
// 日志文件路径 (输出 JSONL)
const std::string kLogFile = "log/rocksdb_ingestion_log_cpp.jsonl"; // 修改为 .jsonl 扩展名
// CSV 读取和批量写入的块大小 (行数)
const int kChunkSize = 10000;

// --- 硬编码的 CSV 列名列表 (用于 JSON 输出的键) ---
// !! IMPORTANT !! 请根据你的 CSV 文件的实际头部精确调整这个列表的顺序和数量
const std::vector<std::string> kJsonKeys = {
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
   "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type",
   "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
   "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"
   // 如果你的 CSV 有更多列，请在这里添加
};
// !! IMPORTANT !! CSVReader 的模板参数需要匹配你打算读取的最大列数
const int kMaxCsvColumns = 19; // 根据上面的 kJsonKeys 数量调整，或者更大一些防止出错

// RocksDB 选项
rocksdb::Options rocksdb_options;

// --- 辅助函数：获取当前时间并格式化为 ISO 8601 字符串 (带毫秒) ---
std::string GetISO8601Timestamp() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    auto timer = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&timer), "%Y-%m-%dT%H:%M:%S"); // 年-月-日 T 时:分:秒
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count(); // 毫秒

    return ss.str();
}


int main() {
    // --- 配置 RocksDB 选项 ---
    rocksdb_options.create_if_missing = true; // 如果数据库目录不存在，则创建
    rocksdb_options.OptimizeLevelStyleCompaction(); // ★★ 修正 ★★ 调用函数
    // 启用 LZ4 压缩 (需要系统中安装了 liblz4 且 RocksDB 编译时支持)
    rocksdb_options.compression = rocksdb::CompressionType::kLZ4Compression; // ★★ 修正 ★★ 枚举名称
    // 如果需要 ZSTD 或 Snappy, 使用 rocksdb::CompressionType::kZSTDCompression 或 kSnappyCompression
    rocksdb_options.max_open_files = 10000;
    rocksdb_options.write_buffer_size = 64 * 1024 * 1024; // 64MB
    rocksdb_options.max_write_buffer_number = 3;
    rocksdb_options.target_file_size_base = 64 * 1024 * 1024; // 64MB


    // --- 确保目录存在 ---
    std::cout << "确保目录存在..." << std::endl;
    try {
        std::filesystem::create_directories(std::filesystem::path(kLogFile).parent_path());
        std::filesystem::create_directories(std::filesystem::path(kDBPath));
        std::cout << "目录确保成功。" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "创建目录时发生错误: " << e.what() << std::endl;
        return 1; // 无法创建目录则退出
    }


    // --- 清理旧数据库 ---
    std::cout << "正在清理旧数据库目录: " << kDBPath << std::endl;
    rocksdb::Status s = rocksdb::DestroyDB(kDBPath, rocksdb_options);
    if (s.ok()) {
        std::cout << "旧数据库清理成功或目录不存在。" << std::endl;
    } else {
        if (!s.IsNotFound()) {
             std::cerr << "清理旧数据库时发生错误: " << s.ToString() << std::endl;
             // return 1; // 如果是严重错误，可以选择退出
        } else {
             std::cout << "数据库目录 " << kDBPath << " 不存在，无需清理。" << std::endl;
        }
    }

    // --- 打开 RocksDB 数据库 ---
    rocksdb::DB* db = nullptr;
    std::cout << "正在打开 RocksDB 数据库: " << kDBPath << std::endl;
    s = rocksdb::DB::Open(rocksdb_options, kDBPath, &db);
    if (!s.ok()) {
        std::cerr << "无法打开数据库: " << s.ToString() << std::endl;
        return 1;
    }
    std::unique_ptr<rocksdb::DB> db_guard(db);
    std::cout << "成功打开 RocksDB 数据库。" << std::endl;

    // --- 打开日志文件 ---
    std::ofstream log_file_stream(kLogFile, std::ios::app); // 以追加模式打开日志文件
    if (!log_file_stream.is_open()) {
        std::cerr << "无法打开日志文件: " << kLogFile << std::endl;
        // return 1; // 日志文件无法打开则退出
    }
    std::cout << "日志将写入到: " << kLogFile << std::endl;


    // --- 打开 CSV 文件并准备读取器 ---
    std::cout << "\n正在打开 CSV 文件: " << kCsvFile << std::endl;
    // CSVReader 模板参数需要精确匹配你读取的最大列数 (kMaxCsvColumns)
    io::CSVReader<kMaxCsvColumns, io::trim_chars<>, io::double_quote_escape<',','\"'>> csv_reader(kCsvFile);

    // 读取 CSV 头，Fast-CPP-CSV-Parser 需要你提供变量来接收头部数据
    // 提供的变量数量需要匹配 kMaxCsvColumns
    // 硬编码的列名列表 kJsonKeys 用于指导 read_header 跳过头部并关联列名
    // 注意：这里的列名需要与你的 CSV 头部精确匹配
    // 硬编码列名变量 (与 kMaxCsvColumns 匹配)
    std::string h[kMaxCsvColumns];
    csv_reader.read_header(
        io::ignore_extra_column | io::ignore_missing_column,
        h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8], h[9],
        h[10], h[11], h[12], h[13], h[14], h[15], h[16], h[17], h[18]
        // 添加或删除这里的变量以匹配 kMaxCsvColumns
    );

    // 可以在这里打印读取到的列名进行验证（虽然库的API不直接返回vector）
    // std::cout << "CSV 头部读取到: ";
    // for(int i = 0; i < kMaxCsvColumns; ++i) std::cout << h[i] << (i == kMaxCsvColumns - 1 ? "" : ", ");
    // std::cout << std::endl;


    std::cout << "CSV 文件打开成功并跳过头部。" << std::endl;


    // --- 批量读取 CSV 数据并写入 RocksDB ---
    std::cout << "\n开始从 CSV 读取数据并批量写入 RocksDB (块大小: " << kChunkSize << ")..." << std::endl;

    auto start_time_total = std::chrono::high_resolution_clock::now();
    long long total_rows_processed = 0; // 使用 long long 存储总行数
    std::chrono::duration<double> total_write_time_taken = std::chrono::duration<double>::zero(); // 累计写入时间
    int chunk_index = 0;

    // 用于接收每一行的列数据 (需要与 kMaxCsvColumns 匹配)
    std::string cols[kMaxCsvColumns];


    // 循环读取 CSV 数据块
    while(true) { // 外层循环控制块
        chunk_index++;
        long long rows_in_this_chunk = 0;
        rocksdb::WriteBatch batch;

        auto start_time_batch = std::chrono::high_resolution_clock::now();

        // 读取一个块的数据并构建 WriteBatch
        for (int i = 0; i < kChunkSize; ++i) {
             // read_row 尝试读取一行，成功返回 true，失败 (文件结束或错误) 返回 false
             // read_row 需要 kMaxCsvColumns 个参数
             if (!csv_reader.read_row(cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8], cols[9],
                                      cols[10], cols[11], cols[12], cols[13], cols[14], cols[15], cols[16], cols[17], cols[18])) {
                 // 如果是文件结束且当前块非空，则处理最后一个块
                 if (rows_in_this_chunk > 0) break;
                 // 否则，文件结束且当前块为空，跳出外层 while 循环
                 else goto end_of_csv_reading; // 使用 goto 跳出两层循环
             }

            rows_in_this_chunk++;
            // 计算当前行的全局行号 (从 0 开始)
            long long global_row_index = total_rows_processed + rows_in_this_chunk - 1;


            // --- 生成键 ---
            // 使用全局行号作为键
            std::string key = "row_" + std::to_string(global_row_index);
             // 如果需要固定宽度键，可以使用 ostrstream 或 printf 格式化


            // --- 将行数据序列化为 JSON 作为值 ---
            nlohmann::json j;
            // 遍历读取到的列数组 cols，并使用 kJsonKeys 作为 JSON 的键
            // 循环次数取 kJsonKeys 和 cols 数组大小的较小值，确保不越界
            for (size_t col_idx = 0; col_idx < kJsonKeys.size() && col_idx < kMaxCsvColumns; ++col_idx) {
                 j[kJsonKeys[col_idx]] = cols[col_idx];
            }

            std::string value_str = j.dump(); // 将 JSON 对象转为字符串

            // 将键值对添加到批量写入中
            batch.Put(key, value_str); // RocksDB Put 接受 string 或 Slice
        } // End of inner loop (building batch)

        // --- 写入整个批量操作到 RocksDB ---
        rocksdb::WriteOptions write_options;
        s = db->Write(write_options, &batch);
        auto end_time_batch = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> time_taken_batch = end_time_batch - start_time_batch;
        total_write_time_taken += time_taken_batch; // 累加总写入时间

        double batch_rate = 0;
        if (time_taken_batch.count() > 0) {
             batch_rate = static_cast<double>(rows_in_this_chunk) / time_taken_batch.count();
        }


        // --- 记录日志 (JSONL) ---
        nlohmann::json log_entry;
        log_entry["timestamp"] = GetISO8601Timestamp();
        log_entry["chunk_index"] = chunk_index;
        log_entry["rows_ingested"] = rows_in_this_chunk;
        log_entry["time_taken_seconds"] = std::round(time_taken_batch.count() * 10000.0) / 10000.0; // 保留4位小数
        log_entry["ingestion_rate_rows_per_sec"] = std::round(batch_rate * 100.0) / 100.0; // 保留2位小数
        // 注意：这里的 total_rows_processed 在写入日志后才会更新
        log_entry["total_rows_ingested_so_far"] = total_rows_processed + rows_in_this_chunk;
        log_entry["total_time_taken_so_far"] = std::round(total_write_time_taken.count() * 10000.0) / 10000.0; // 保留4位小数

        // 填充系统指标和磁盘 I/O 占位符 (在 C++ 中获取这些需要 OS 特定 API，这里仅为格式占位)
        log_entry["system_metrics_after_chunk"]["cpu_percent"] = -1.0;
        log_entry["system_metrics_after_chunk"]["memory_percent"] = -1.0;
        log_entry["system_metrics_after_chunk"]["memory_used_gb"] = -1.0;
        log_entry["disk_io_delta_during_chunk_bytes"]["read"] = 0;
        log_entry["disk_io_delta_during_chunk_bytes"]["write"] = 0;
        log_entry["disk_io_delta_during_chunk_count"]["read"] = 0;
        log_entry["disk_io_delta_during_chunk_count"]["write"] = 0;


        if (s.ok()) {
            log_entry["status"] = "SUCCESS";
            // 打印进度和当前批次速率到控制台
            std::cout << "处理块 " << chunk_index << " (" << rows_in_this_chunk << " 行)... 完成。"
                      << " 耗时: " << std::fixed << std::setprecision(4) << time_taken_batch.count() << " 秒，速率: "
                      << std::fixed << std::setprecision(2) << batch_rate << " 行/秒。" << std::endl;
            std::cout << "  -> 累计插入: " << total_rows_processed + rows_in_this_chunk << " 行。" << std::endl;
        } else {
            log_entry["status"] = "ERROR";
            log_entry["error"] = s.ToString();
            std::cerr << "处理块 " << chunk_index << " 时发生 RocksDB 错误: " << s.ToString() << std::endl;
            std::cerr << "  -> 块 " << chunk_index << " 插入失败。" << std::endl;
            // 如果错误严重，可以选择在这里跳出循环或直接退出
            // break;
        }

        // 将 JSONL 写入日志文件
        log_file_stream << log_entry.dump() << '\n';
        log_file_stream.flush(); // 立即写入到文件


        total_rows_processed += rows_in_this_chunk;

        // read_row 遇到文件尾或错误会返回 false，内层循环会跳出
        // 如果内层循环读完一个完整的 chunk (rows_in_this_chunk == kChunkSize)，外层 while 继续
        // 如果内层循环读到文件尾或错误 (rows_in_this_chunk < kChunkSize)，break 跳出内层循环
        // 然后外层 while 检查条件，如果是文件尾，之前设置的 goto 会跳出。
        // 如果是错误，可以通过检查 s.ok() 来决定是否跳出外层循环。
        // 当前逻辑是遇到文件尾小于 kChunkSize 时，break 内层循环，然后 goto 跳出外层。
        // 遇到错误，打印错误日志，然后 continue 外层循环（处理下一个块，如果还有的话）。
        // 你可能需要调整这里的错误处理逻辑。

    } // End of outer while loop

end_of_csv_reading:; // goto 标签


    auto end_time_total = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> actual_total_time = end_time_total - start_time_total; // 包含读取和写入的总时间


    // --- 总结 ---
    double overall_avg_write_rate = 0;
    if (total_write_time_taken.count() > 0) {
        overall_avg_write_rate = static_cast<double>(total_rows_processed) / total_write_time_taken.count();
    }

    std::cout << "\n--- 导入总结 ---" << std::endl;
    std::cout << "总共插入行数: " << total_rows_processed << std::endl;
    std::cout << "总处理 (读取+写入) 耗时: " << std::fixed << std::setprecision(4) << actual_total_time.count() << " 秒" << std::endl;
    std::cout << "总写入 RocksDB 耗时: " << std::fixed << std::setprecision(4) << total_write_time_taken.count() << " 秒" << std::endl;
    std::cout << "整体平均写入 RocksDB 速率: " << std::fixed << std::setprecision(2) << overall_avg_write_rate << " 行/秒" << std::endl;
    std::cout << "详细日志已保存到: " << kLogFile << std::endl;


    // db_guard 在这里超出作用域，会自动调用 db->Close()
    log_file_stream.close(); // 关闭日志文件

    return 0;
}