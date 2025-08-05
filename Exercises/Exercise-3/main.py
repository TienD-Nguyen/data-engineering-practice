import boto3
import os
import botocore
from botocore import UNSIGNED
from botocore.config import Config
import gzip
import sys

def log_info(message):
    print(f"[INFO] - {message}")
    
def log_error(message):
    print(f"[ERROR] - {message}")

def ensure_directory(directory_name):
    if not os.path.exists(directory_name):
        log_info(f"Directory '{directory_name}' does not exist, creating it.")
        os.makedirs(directory_name, exist_ok=True)
        
def download_file_from_s3(bucket_name, object_key, download_directory):
    localfile_name = os.path.basename(object_key)
    localfile_path = os.path.join(download_directory, localfile_name)
    
    log_info(f"Checking local directory for existing file: '{localfile_name}'")
    if os.path.exists(localfile_path):
        print(f"[INFO] - File already exists: '{localfile_name}'")
        return localfile_path
    print(f"[INFO] - File does not exist, proceeding with download")
    
    ensure_directory(download_directory)
    
    s3 = boto3.client('s3')
    
    try:
        log_info(f"Downloading '{object_key}' from bucket '{bucket_name}' to '/{download_directory}'.")
        s3.download_file(bucket_name, object_key, localfile_path)
        print(f"[OK] - File downloaded successfully: {localfile_name}")
        return localfile_path
    except Exception as e:
        log_error(f"Failed to download file from S3: {e}")
        raise
    
def read_first_line_from_file_s3(bucket_name: str, object_key: str):
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        gzip_body = response["Body"]
        
        with gzip.GzipFile(fileobj=gzip_body) as gz:
            content = gz.readline().decode('utf-8').strip()
            log_info(f"First line: {content}")
            return content
    except botocore.exceptions.ClientError as e:
        log_error(f"Failed to read first line from S3 gzip file: {e}")
        raise
    
def read_gzip_file(file_path, read_first: bool):
    if not os.path.exists(file_path):
        log_error(f"File does not exist: {file_path}")
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    try:
        with gzip.open(file_path, "rt") as f:
            if read_first:
                log_info(f"Reading first line from gzip file: {file_path}")
                uri = f.readline().strip()
                log_info(f"Read URI from gzip file: {uri}")
                return uri
            else:
                log_info(f"Reading all lines from gzip file: {file_path}")
                for line in f.readlines():
                    print(line.strip())
    except Exception as e:
        log_error(f"Failed to read gzip file: {e}")
        raise
    
def stream_gzip_file_from_s3(bucket_name, object_key):
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        body = response['Body']
        log_info(f"Streaming gzip file '{object_key}' from bucket '{bucket_name}'.")
        
        with gzip.GzipFile(fileobj=body) as gz:
            decompressed_chunks = []
            while True:
                chunk = gz.read(1024)
                if not chunk:
                    break
                sys.stdout.write(chunk.decode('utf-8', errors='replace'))
        #         decompressed_chunks.append(chunk)
        # file_content = b''.join(decompressed_chunks).decode('utf-8')
        # return file_content
    except botocore.exceptions.ClientError as e:
        log_error(f"Failed to stream gzip file from S3: {e}")
        raise
        
def main():
    BUCKET_NAME = "commoncrawl"
    OBJECT_KEY = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
    
    # log_info("Starting the Common Crawl data download process. Looking for '{OBJECT_KEY}' in bucket '{BUCKET_NAME}'.")
    # local_filepath = download_file_from_s3(bucket_name=BUCKET_NAME, 
    #                                         object_key=OBJECT_KEY, 
    #                                         download_directory=DOWNLOAD_DIR)
    
    # uri = read_gzip_file(local_filepath, read_first=True)
    
    # cc_main_file = download_file_from_s3(bucket_name=BUCKET_NAME, 
    #                                         object_key=uri, 
    #                                         download_directory=DOWNLOAD_DIR)
    # contents = read_gzip_file(cc_main_file, read_first=False)
    
    log_info(f"Reading content from gzip file '{OBJECT_KEY}' in bucket '{BUCKET_NAME}'.")
    uri = read_first_line_from_file_s3(bucket_name=BUCKET_NAME, object_key=OBJECT_KEY)
    log_info(f"URI extracted from reading gzip file: {uri}. Looking for the file in the same bucket.")
    stream_gzip_file_from_s3(bucket_name=BUCKET_NAME, object_key=uri)

if __name__ == "__main__":
    main()
