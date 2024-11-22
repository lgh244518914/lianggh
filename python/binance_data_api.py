import os
import re
import requests
import zipfile
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://data.binance.vision/"
PREFIX = "data/futures/um/daily/aggTrades/"


class BinanceDataAPI:
    def __init__(self, save_dir):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def get_data(self, target_date):
        """获取指定日期的 Binance 数据."""
        folders = self._get_usdt_folders()
        for folder in folders:
            self._process_data(folder, target_date)

    def _get_usdt_folders(self):
        """获取所有以 USDT 结尾的文件夹."""
        url = f"{BASE_URL}?prefix={PREFIX}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("无法访问目标网址")

        soup = BeautifulSoup(response.text, "html.parser")
        usdt_folders = []
        for link in soup.find_all("a"):
            href = link.get("href")
            # 筛选符合 USDT 文件夹规则的链接
            if href and href.endswith("USDT/"):
                usdt_folders.append(href)

        print(f"发现文件夹: {usdt_folders}")
        return usdt_folders

    def _process_data(self, folder, date):
        """下载和处理某个文件夹中的数据."""
        file_name = f"{date}.zip"
        file_url = f"{BASE_URL}{PREFIX}{folder}{file_name}"
        save_path = os.path.join(self.save_dir, folder.strip("/"), file_name)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # 下载文件
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"下载完成: {save_path}")
            extract_dir = self._extract_zip(save_path)
            self._process_and_save_as_pickle(extract_dir)
        else:
            print(f"文件不存在: {file_url}")

    def _extract_zip(self, zip_path):
        """解压 ZIP 文件."""
        extract_dir = os.path.splitext(zip_path)[0]
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
            print(f"解压完成: {extract_dir}")
        return extract_dir

    def _process_and_save_as_pickle(self, extract_dir):
        """读取解压数据并保存为 .pickle 格式."""
        files = [os.path.join(extract_dir, f) for f in os.listdir(extract_dir) if f.endswith(".csv")]
        for file in files:
            df = pd.read_csv(file)
            # 数据清洗：删除重复行
            df = df.drop_duplicates()
            pickle_path = file.replace(".csv", ".pickle")
            df.to_pickle(pickle_path)
            print(f"保存为 .pickle: {pickle_path}")


if __name__ == "__main__":
    save_directory = "./binance_data"
    api = BinanceDataAPI(save_directory)
    target_date = "20240102"
    api.get_data(target_date)
