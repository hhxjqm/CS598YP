# 使用 Python 3.11 的 slim 镜像作为基础
FROM python:3.11-slim

# 更新 apt 包列表并安装所需的系统依赖
# build-essential: 用于编译一些 Python 包
# wget: 用于下载文件 (如果需要的话，当前脚本未使用但保留)
# libsqlite3-dev: 安装 SQLite 开发库，确保 Python 的 sqlite3 模块能正常工作
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 库
# --no-cache-dir: 不缓存 pip 包，减小镜像大小
# duckdb, pandas, numpy, psutil: 你的脚本中用到的库
# tqdm: 进度条库 (你的脚本中未使用但保留)
# 注意: python 的 sqlite3 模块是内置的，不需要在这里 pip install
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
CMD ["bash"]
