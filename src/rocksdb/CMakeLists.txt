cmake_minimum_required(VERSION 3.14)
project(rocksdb_tools)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找系统安装的 RocksDB
find_library(ROCKSDB_LIB rocksdb REQUIRED)
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h REQUIRED)

# query_test 可执行程序
add_executable(query_test
    query_test.cpp
)

# ingestion_test_streamwrite 可执行程序
add_executable(ingestion_test_streamwrite
    ingestion_test_streamwrite.cpp
)

# 指定头文件目录 (给所有target都加上)
target_include_directories(query_test PRIVATE ${ROCKSDB_INCLUDE_DIR} /usr/local/include)
target_include_directories(ingestion_test_streamwrite PRIVATE ${ROCKSDB_INCLUDE_DIR} /usr/local/include)

# 链接 RocksDB库 + pthread (每个target都链接)
target_link_libraries(query_test PRIVATE ${ROCKSDB_LIB} pthread)
target_link_libraries(ingestion_test_streamwrite PRIVATE ${ROCKSDB_LIB} pthread)
