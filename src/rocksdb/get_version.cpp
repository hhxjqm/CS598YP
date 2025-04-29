#include <rocksdb/version.h>
#include <iostream>
int main() {
    std::cout << ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH << "\n";
}
