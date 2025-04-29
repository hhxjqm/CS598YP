#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <json.hpp>

using json = nlohmann::json;

// 逐行读取 CSV，每行即写入 RocksDB
void stream_insert_csv_to_rocksdb(const std::string& csv_file, const std::string& db_path) {
    std::ifstream file(csv_file);
    if (!file.is_open()) throw std::runtime_error("❌ 无法打开 CSV 文件");

    std::string header_line, line;
    std::getline(file, header_line);

    std::vector<std::string> headers;
    std::stringstream ss(header_line);
    std::string col;
    while (std::getline(ss, col, ',')) {
        std::transform(col.begin(), col.end(), col.begin(), ::tolower);
        headers.push_back(col);
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db;
    auto status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) throw std::runtime_error("❌ RocksDB 打开失败: " + status.ToString());

    rocksdb::WriteBatch batch;
    size_t count = 0;

    while (std::getline(file, line)) {
        std::stringstream ls(line);
        std::string cell;
        json row;
        size_t idx = 0;

        while (std::getline(ls, cell, ',') && idx < headers.size()) {
            row[headers[idx++]] = cell;
        }

        std::string key = "key_" + std::to_string(count);
        batch.Put(key, row.dump());

        if (++count % 1000 == 0) {  // 每1000行批量写一次
            db->Write(rocksdb::WriteOptions(), &batch);
            batch.Clear();
        }
    }

    if (batch.Count() > 0) {
        db->Write(rocksdb::WriteOptions(), &batch);
    }

    delete db;
    std::cout << "✅ 插入完成，共 " << count << " 行\n";
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "用法: ./insert_all <csv_path> <rocksdb_path>\n";
        return 1;
    }

    try {
        stream_insert_csv_to_rocksdb(argv[1], argv[2]);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }

    return 0;
}
