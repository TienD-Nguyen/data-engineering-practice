from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as psf
import logging
import zipfile
from io import BytesIO
import os
import re

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(message)s')
logger = logging.getLogger()

#-----Support function-----
def retrieve_zip_file_path_and_name(directory: str):
    zip_files = []
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.lower().endswith(".zip"):
                zip_files.append([filename, os.path.join(dirpath, filename)])
    return zip_files

def safe_decode(byte_content: bytes):
    try:
        return byte_content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return byte_content.decode("utf-8-sig")
        except UnicodeDecodeError:
            return byte_content.decode("ISO-8859-1")

#-----Data Loading Functions-----
def read_zip_content_into_memory(zip_file: str):
    contents = []
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        for name in zip_ref.namelist():
            if name.startswith("__MACOSX") or name.endswith(".DS_Store") or "/._" in name:
                continue
            if name.endswith(".csv"):
                with zip_ref.open(name) as f:
                    contents.append(BytesIO(f.read()))
    return contents
        # return [zip_ref.read(x) for x in zip_ref.namelist() if re.fullmatch(r"[^/]*\.csv", x)]

def create_spark_dataframe_from_memory(csv_io: BytesIO, sc: SparkSession, header: bool, infer_schema: bool):
    csv_io.seek(0)
    csv_bytes = csv_io.read()
    csv_str = safe_decode(csv_bytes)
    csv_lines = csv_str.splitlines()

    rdd = sc.sparkContext.parallelize(csv_lines)

    df = sc.read.csv(rdd, header=header, inferSchema=infer_schema)
    return df

def main():
    # Initialize Spark Session
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    # Processing and Loading Data from Zip files
    zip_files = retrieve_zip_file_path_and_name(os.path.join(os.getcwd(), "data"))
    for zip_filename, zip_path in zip_files:
        zip_io_content = read_zip_content_into_memory(zip_path)[0]
        spark_df = create_spark_dataframe_from_memory(zip_io_content, sc=spark, header=True, infer_schema=True)
        
        # Add columns based on the task's requirement
        spark_df = spark_df.withColumn("source_file", psf.lit(zip_filename))\
            .withColumn("file_date", psf.to_date(psf.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)))\
            .withColumn("brand", psf.when(psf.size(psf.split("model", " ")) > 1, psf.split("model", " ")[0]).otherwise("unknown"))

        # Create additional dataframe for ranking of models' storage
        temp_df = spark_df.select(["model", "capacity_bytes"])\
            .withColumn("model", psf.regexp_replace("model", r"\s{2,}", r"\s"))\
            .withColumn("capacity_bytes", psf.col("capacity_bytes").astype("long"))\
            .drop_duplicates()\
            .withColumn("storage_ranking", psf.dense_rank().over(Window.orderBy(psf.col("capacity_bytes").desc())))
        
        # Join dataframe on "model" and proceed with the last task of creating "primary key"
        spark_df = spark_df.join(temp_df.select(["model", "storage_ranking"]), on="model", how="left")\
            .withColumn("primary_key", psf.hash("model", "serial_number"))

        spark_df.select(["date", "serial_number", "model", "capacity_bytes", "failure", "source_file", "file_date", "brand", "storage_ranking", "primary_key"]).show()

    # your code here


if __name__ == "__main__":
    main()
