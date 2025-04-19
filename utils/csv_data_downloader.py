# csv_data_downloader.py

import os
import argparse
import requests
from urllib.parse import urlparse
from tqdm import tqdm

def download_file(url, save_dir="data_set"):
    os.makedirs(save_dir, exist_ok=True)

    # 从 URL 中提取文件名
    filename = os.path.basename(urlparse(url).path)
    save_path = os.path.join(save_dir, filename)

    if os.path.exists(save_path):
        print(f"✅ 文件已存在: {save_path}")
        return

    print(f"⬇️ 正在下载: {url}")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get('content-length', 0))
            with open(save_path, 'wb') as f, tqdm(
                desc=filename,
                total=total,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))
        print(f"✅ 下载完成: {save_path}")
    except Exception as e:
        print(f"❌ 下载失败: {e}")

def main():
    parser = argparse.ArgumentParser(description="下载一个 CSV 文件到 data_set 文件夹")
    parser.add_argument('--csv_url', required=True, help='CSV 文件的完整 URL')
    args = parser.parse_args()

    download_file(args.csv_url)

if __name__ == "__main__":
    main()
