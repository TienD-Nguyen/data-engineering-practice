import os
import requests
from urllib.parse import urlparse
from zipfile import ZipFile
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def create_directory(directory):
    os.makedirs(directory, exist_ok=True)

def extract_and_cleanup(zip_path, extract_dir):
    create_directory(extract_dir)
    
    try:
        with ZipFile(zip_path, "r") as zip_ref:
            for file in zip_ref.namelist():
                if file.lower().endswith(".csv"):
                    zip_ref.extract(file, extract_dir)
                    print(f"Extracted {file} to {extract_dir}")
    except Exception as e:
        print(f"[Error] - Failed to extract {zip_path}: {e}")
    finally:
        os.remove(zip_path)
        print(f"[OK] - Removed zip file {zip_path}")

async def download_all_files(uris, download_dir):
    async with aiohttp.ClientSession() as session:
        tasks = [concurrent_downloads(session, uri, download_dir) for uri in uris]
        return await asyncio.gather(*tasks)

async def concurrent_downloads(session, url, download_dir):
    create_directory(download_dir)
    
    filename = os.path.basename(urlparse(url).path)
    file_path = os.path.join(download_dir, filename)
    
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            with open(file_path, "wb") as file:
                async for chunk in response.content.iter_chunked(8192):
                    file.write(chunk)
        print(f"[OK] - Downloaded {filename}")
        return file_path
    except Exception as e:
        print(f"[Error] - Failed to download {filename}: {e}")
        return None
    
def extract_all(downloaded_files, extract_dir):
    with ThreadPoolExecutor() as executor:
        for zip_path in downloaded_files:
            executor.submit(extract_and_cleanup, zip_path, extract_dir)
    
def main():
    download_dir = os.path.join(os.getcwd(), "downloads")
    
    downloaded_files = asyncio.run(download_all_files(download_uris, download_dir))
    downloaded_files = [f for f in downloaded_files if f]
    
    extract_all(downloaded_files, download_dir)
    

def download_file(url, download_dir):
    create_directory(download_dir)
    
    filename = os.path.basename(urlparse(url).path)
    file_path = os.path.join(download_dir, filename)
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get("content-length", 0))
    download_size = 0
    with open(file_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
            
            download_size += len(chunk)
            if total_size > 0 and download_size % (1024 * 1024) == 0:
                progress = (download_size / total_size) * 100
                print(f"   {filename}: {progress:.2f}% ({download_size} / {total_size} bytes)")
            
    print(f"Downloaded {filename}")
    return file_path

def sequential_main():
    download_dir = os.path.join(os.getcwd(), "downloads")
    
    for uri in download_uris:
        try:
            zip_path = download_file(uri, download_dir)
            extract_and_cleanup(zip_path, download_dir)
        except requests.RequestException as e:
            print(f"Failed to download {uri}: {e}")

    
if __name__ == "__main__":
    main()
