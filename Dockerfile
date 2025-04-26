# Dockerfile

FROM python:3.11-slim

# 替换源为 bookworm 的阿里云源（重点是这一段）
RUN rm -f /etc/apt/sources.list && \
    rm -rf /etc/apt/sources.list.d/* && \
    touch /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian/ bookworm main non-free contrib" > /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian/ bookworm-updates main non-free contrib" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian-security bookworm-security main" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian bookworm-backports main non-free contrib" >> /etc/apt/sources.list


RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# 换国内 pip 源并限制下载超时
RUN pip install --upgrade pip -i https://mirrors.aliyun.com/pypi/simple --timeout 60 && \
    pip install --no-cache-dir \
        duckdb \
        pandas \
        numpy \
        tqdm \
        psutil \
        matplotlib \
        -i https://mirrors.aliyun.com/pypi/simple --timeout 60


WORKDIR /test

COPY monitor_ingestion.py /test/
CMD ["tail", "-f", "/dev/null"]

