import requests
import pandas
import os
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import sys
import random

URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
TARGET_DATETIME = "2024-01-19 15:45"

def fetch_data(url, modified_datetime):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        soup = BeautifulSoup(response.content, "html.parser")
        
        download_links = []
        for row in soup.find_all("tr"):
            if modified_datetime in row.get_text():
                link_tag = row.find("a", href=True)
                if link_tag:
                    link = url + link_tag["href"]
                    download_links.append(link)
                    print(f"[INFO] - Found download link for {link_tag['href']} at {modified_datetime}")
        return download_links
    except requests.RequestException as e:
        print(f"[ERROR] -  Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] -  An unexpected error occurred: {e}")
        return None
    
def create_directory(directory):
    os.makedirs(directory, exist_ok=True)
    
async def download_file(session, url, save_path):
    create_directory(directory=save_path)
    
    filename = os.path.basename(urlparse(url).path)
    file_path = os.path.join(save_path, filename)
    
    if os.path.exists(file_path):
        print(f"[INFO] - File {filename} already exists. Skipping download.")
        return file_path
    
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            with open(file_path, "wb") as file:
                async for chunk in response.content.iter_chunked(8192):
                    file.write(chunk)
        print(f"[OK] - Downloaded {filename}")
        return file_path
    except Exception as e:
        print(f"[ERROR] - Failed to download {filename}: {e}")
        
async def download_files_concurrently(urls, save_path):
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url, save_path) for url in urls]
        return await asyncio.gather(*tasks)
    
def load_csv_and_find_max_temp(csv_list: list):
    if not csv_list:
        print(f"[ERROR] - No CSV files provided for processing.")
        return None
    
    csv_file = random.choice(csv_list)
    print(f"[INFO] - Reading file: {os.path.basename(csv_file)}")
    
    try:
        df = pandas.read_csv(csv_file)
        max_temp = df["HourlyDryBulbTemperature"].max()
        max_rows = df[df["HourlyDryBulbTemperature"] == max_temp]
        
        print(f"[INFO] - Max HourlyDryBulbTemperature: {max_temp}")
        print(max_rows)
    except Exception as e:
        print(f"[ERROR] - Failed to read CSV file {os.path.basename(csv_file)}: {e}")
        return None

def main():
    # your code here
    download_links = fetch_data(URL, TARGET_DATETIME)
    if not download_links:
        print("[ERROR] - No links found for the specified datetime. Exiting...")
        sys.exit(1)
    print(f"[INFO] - Found {len(download_links)} links to download.")
    
    download_dir = os.path.join(os.getcwd(), "downloads")
    downloaded_files = asyncio.run(download_files_concurrently(download_links, download_dir))
    downloaded_files = [file for file in downloaded_files if file is not None]
    
    load_csv_and_find_max_temp(downloaded_files)

if __name__ == "__main__":
    main()
