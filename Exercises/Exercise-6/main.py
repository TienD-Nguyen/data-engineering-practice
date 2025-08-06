from pyspark.sql import SparkSession
import os

def log_info(message):
    print(f"[INFO] - {message}")

def log_error(message):
    print(f"[ERROR] - {message}")

def retrieve_zip_file_name(directory: str):
    zip_files = []
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.lower().endswith(".zip"):
                zip_files.append(os.path.join(dirpath, filename))
    return zip_files

def ensure_directory(directory: str):
    log_info(f"Checking for directory '{directory}'.")
    path = os.path.join(os.getcwd(), directory)
    if not os.path.exists(path):
        log_info(f'No directory exists for {directory}. Creating one in current work directory.')
        os.makedirs(name=path, exist_ok=True)
    log_info(f"The directory is exist.")
    return path

def read_csv_from_zip_files(spark: SparkSession, zip_file_paths: list, header=True, infer_schema=True):
    df = spark.read.csv(path=zip_file_paths, header=header, inferSchema=infer_schema)
    return df

def main():
    REPORT_DIR = "reports"

    for dirpath, dirnames, filenames in os.walk(os.path.join(os.getcwd(), "data")):
        print(dirpath)
        print(dirnames)
        print(filenames)
    # report_dir = ensure_directory(directory=REPORT_DIR)
    # spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    zip_files = retrieve_zip_file_name(os.path.join(os.getcwd(), "data"))
    print(zip_files)
    # trips_df = read_csv_from_zip_files(spark=spark, zip_file_paths=zip_files)
    # trips_df.show(5)
    # trips_df.printSchema()


if __name__ == "__main__":
    main()
