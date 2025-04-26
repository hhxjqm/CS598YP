# 使用 Python 3.11 的 slim 镜像作为基础
FROM python:3.11-slim

# 设置非交互模式，避免 apt-get 在安装过程中提问
ENV DEBIAN_FRONTEND=noninteractive

# 更新 apt 包列表并安装所需的系统依赖
# build-essential: 用于编译一些 Python 包
# wget: 用于下载文件 (如果需要的话)
# libsqlite3-dev: 确保 Python 的 sqlite3 模块能正常工作
# librocksdb-dev: ★★ 新增 ★★ RocksDB 开发库 (包含头文件和静态/动态库文件)
# rocksdb-tools: ★★ 新增 ★★ RocksDB 命令行工具 (如 ldb)
# --no-install-recommends: 可选，用于避免安装推荐的额外包，减小镜像大小
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    libsqlite3-dev \
    librocksdb-dev \
    rocksdb-tools \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 库
# --no-cache-dir: 不缓存 pip 包，减小镜像大小
# duckdb, pandas, numpy, psutil, matplotlib: 你的脚本中用到的库
# tqdm: 进度条库 (你的脚本中可能未使用但保留)
# 注意: python 的 sqlite3 模块是内置的，libsqlite3-dev 是为了确保其编译或链接正常
RUN pip install --no-cache-dir \
    duckdb \
    pandas \
    numpy \
    tqdm \
    psutil \
    matplotlib

# 设置工作目录
WORKDIR /test

# 将当前目录的所有文件复制到容器的 /test 目录
COPY . /test

# 容器启动时执行的默认命令
# 你可能需要根据你的实际需求修改这个命令
CMD ["bash"]