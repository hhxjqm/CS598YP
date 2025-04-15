FROM python:3.11-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 库
RUN pip install --no-cache-dir \
    duckdb \
    pandas \
    numpy \
    tqdm \
    psutil

# 设置工作目录
WORKDIR /test

# 拷贝写入测试脚本
COPY monitor_ingestion.py /test/

# 默认执行写入测试脚本
CMD ["python", "monitor_ingestion.py"]
