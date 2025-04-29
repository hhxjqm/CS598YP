FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    ca-certificates \
    libsqlite3-dev \
    librocksdb-dev \
    rocksdb-tools \
    cmake \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    duckdb \
    pandas \
    numpy \
    tqdm \
    psutil \
    matplotlib

# 下载 nlohmann/json 单头文件
RUN wget https://github.com/nlohmann/json/releases/download/v3.11.2/json.hpp -O /usr/local/include/json.hpp


WORKDIR /test

CMD ["bash"]
