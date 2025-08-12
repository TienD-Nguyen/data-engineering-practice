from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import LongType
import pyspark.sql.functions as psf
import os
import zipfile
import pandas as pd
from io import BytesIO, StringIO
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(message)s')
logger = logging.getLogger()

#-----Support functions-----
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

def create_directory(directory: str):
    logger.info(f"Checking for directory '{directory}'.")
    path = os.path.join(os.getcwd(), directory)
    if not os.path.exists(path):
        logger.info(f'No directory exists for {directory}. Creating one in current work directory.')
        os.makedirs(name=path, exist_ok=True)
    logger.info(f"The directory is exist.")
    return path

def safe_decode(byte_content: bytes):
    try:
        return byte_content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return byte_content.decode("utf-8-sig")
        except UnicodeDecodeError:
            return byte_content.decode("ISO-8859-1")

#-----Data loading functions-----
def read_zip_content_into_memory(zipfile_path: str):
    csv_files = []
    with zipfile.ZipFile(zipfile_path, "r") as zip_ref:
        for name in zip_ref.namelist():
            if name.startswith("__MACOSX") or name.endswith(".DS_Store") or "/._" in name:
                continue  # Skip Mac system files in my situation of using two OS.

            if name.endswith('.csv'):
                with zip_ref.open(name) as f:
                    csv_bytes = BytesIO(f.read())
                    csv_files.append((name, csv_bytes))
    return csv_files
    
def create_spark_dataframe_from_memory(csv_io: BytesIO, sc: SparkSession, header: bool, infer_schema: bool):
    csv_io.seek(0)
    csv_bytes = csv_io.read()
    csv_str = safe_decode(csv_bytes)
    csv_lines = csv_str.splitlines()

    rdd = sc.sparkContext.parallelize(csv_lines)
    
    df = sc.read.csv(rdd, header=header, inferSchema=infer_schema)
    return df

def read_data_into_spark(zip_file_paths: list, sc: SparkSession):
    csv_frames = {}
    for zip_path in zip_file_paths:
        for filename, csv_io in read_zip_content_into_memory(zip_path):
            logger.info(f"Reading files in {zip_path} from memory")
            
            temp_spark_df = create_spark_dataframe_from_memory(csv_io, sc=sc, header=True, infer_schema=True)

            date_column = ("started_at" 
                           if "started_at" in temp_spark_df.columns 
                           else "start_time" 
                           if "start_time" in temp_spark_df.columns 
                           else None)

            if date_column:
                temp_spark_df = temp_spark_df.withColumn("date", psf.to_date(psf.col(date_column)))
            csv_frames[filename] = temp_spark_df.cache()
    return csv_frames

#-----Questions Answering Service-----
class AnsweringService:
    def __init__(self, spark_df: DataFrame, dataset_name: str, spark: SparkSession):
        self.spark_df = spark_df
        self.dataset_name = dataset_name
        self.spark = spark

    def question_one_solution(self, file_path: str):
        logger.info("Answering Question 1 - What is the `average` trip duration per day?")
        sdf = self.spark_df
        if self.dataset_name == "Divvy_Trips_2020_Q1.csv":
            sdf = sdf.select("*", (psf.unix_timestamp(psf.to_timestamp("ended_at")) - psf.unix_timestamp(psf.to_timestamp("started_at"))).alias("tripduration"))

        sdf = sdf.withColumn("tripduration", psf.regexp_replace(psf.col("tripduration"), ",", "").cast("double"))    
        result_df = sdf.groupby("date").agg(psf.round(psf.mean("tripduration"), 2).alias("average_trip_duration"))

        if os.path.exists(file_path):
            logger.info(f"{os.path.basename(file_path)} exists. Appending contents into the file.")
            result_df.repartition(1).write.mode("append").csv(file_path, header=True)
        else:
            logger.info(f"{os.path.basename(file_path)} does not exist. Creating and writing the contents into the file.")
            result_df.repartition(1).write.mode("overwrite").csv(file_path, header=True)

    def question_two_solution(self, file_path: str):
        logger.info("Answering Question 2 - How many trips were taken each day?")
        sdf = self.spark_df
        result_df = sdf.groupBy("date").count()

        if os.path.exists(file_path):
            logger.info(f"{os.path.basename(file_path)} exists. Combining results.")
            existing_df = self.spark.read.csv(file_path, header=True)
            existing_df.cache().count()
            result_df.cache().count()
            existing_df.unionByName(result_df).repartition(1).write.mode("overwrite").csv(file_path, header=True)
        else:
            logger.info(f"{os.path.basename(file_path)} does not exist. Creating and writing contens into the file.")
            result_df.repartition(1).write.mode("overwrite").csv(file_path, header=True)

    def question_three_solution(self, file_path: str):
        logger.info("Answering Question 3 - What was the most popular starting trip station for each month?")

        monthly_count = self.spark_df.select("*",
                                        psf.col(*{"start_station_id", "from_station_id"} & set(self.spark_df.columns)).alias("start_location_id"),
                                        psf.col(*{"start_station_name", "from_station_name"} & set(self.spark_df.columns)).alias("start_location_name"),
                                        *[func(psf.col(x)).alias(y)
                                        for x, y, func in zip(
                                            ["date", "date"], ["year", "month"], [psf.year, psf.month]
                                        )]
        )\
        .groupby("year", "month", "start_location_id", "start_location_name")\
        .agg(psf.count("*").alias("trip_count"))

        window_spec = Window.partitionBy("year", "month").orderBy(psf.desc("trip_count"))
        ranked_df = monthly_count.withColumn("rank", psf.rank().over(window_spec))
        result_df = ranked_df.filter(psf.col("rank") == 1).drop("rank")

        if os.path.exists(file_path):
            logger.info(f"'{os.path.basename(file_path)}' exists. Combining results.")
            existing_df = self.spark.read.csv(file_path, header=True)
            existing_df.cache().count()
            result_df.cache().count()
            existing_df.unionByName(result_df).repartition(1).write.mode("overwrite").csv(file_path, header=True)
        else:
            logger.info(f"'{os.path.basename(file_path)}' does not exist. Creating and writing contens into the file.")
            result_df.repartition(1).write.mode("overwrite").csv(file_path, header=True)

    def question_four_solution(self, file_path: str):
        logger.info("Answering Question 4 - What were the top 3 trip stations each day for the last two weeks?")

        if self.dataset_name == "Divvy_Trips_2020_Q1.csv":
            window = Window.partitionBy("date").orderBy("count")
            date_cutoff = self.spark_df.select(
                psf.date_add(psf.max("date"), -14).alias("max_date")).collect()[0]["max_date"]

            self.spark_df.where(psf.col("date") >= date_cutoff)\
                .groupby("date", "start_station_name")\
                    .count()\
                        .withColumn("rank", psf.row_number().over(window))\
                            .where(psf.col("rank") <= 3)\
                                .write.mode("overwrite").csv(file_path, header=True)

    def question_five_solution(self, file_path: str):
        logger.info("Answering Question 5 - Do `Male`s or `Female`s take longer trips on average?")

        if self.dataset_name == "Divvy_Trips_2019_Q4.csv":
            sdf = self.spark_df.withColumn("tripduration", psf.regexp_replace(psf.col("tripduration"), ",", "").cast("double"))
            sdf.dropna(subset=["tripduration", "gender"]).groupby("gender").agg(
                psf.round(psf.mean("tripduration"), 2).alias("avg_tripduration")
            )\
            .selectExpr("max_by(gender, avg_tripduration) as longest_trip_takers")\
            .repartition(1)\
            .write.mode("overwrite").csv(file_path, header=True)

    def question_six_solution(self, file_path: str):
        logger.info("Answering Question 6 - What is the top 10 ages of those that take the longest trips, and shortest?")

        if self.dataset_name == "Divvy_Trips_2019_Q4.csv":
            sdf = self.spark_df.withColumn("tripduration", psf.regexp_replace(psf.col("tripduration"), ",", "").cast("double"))
            sdf.orderBy(psf.desc("tripduration")).dropna(subset=["birthyear"])\
                .limit(10)\
                    .select((2025 - psf.col("birthyear")).alias("age"), psf.col("tripduration"))\
                        .repartition(1)\
                        .write.mode("overwrite")\
                        .csv(file_path, header=True)

#-----Exploratory function-----
def read_csv_from_zip(zip_path: str, csv_filename=None):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
        if not csv_files:
            log_error(f"No CSV files found in {zip_path}")
            return None
        
        target_file =  csv_filename if csv_filename and csv_filename in csv_files else csv_files[0]

        log_info(f"Reading: {target_file} from {zip_path}")
        print("-" * 50)

        with zip_ref.open(target_file) as file:
            csv_content = file.read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))

        print("\nColumn Information:")
        print(df.dtypes)
        
        print("\nFirst 5 rows:")
        print(df.head())
        
        print("\nBasic Statistics:")
        print(df.describe())
        
        print("\nMissing Values:")
        missing = df.isnull().sum()
        if missing.any():
            print(missing[missing > 0])
        else:
            print("No missing values found")
            
        # Show available CSV files if multiple exist
        if len(csv_files) > 1:
            print(f"\nOther CSV files in zip: {[f for f in csv_files if f != target_file]}")
        
        return df

def main():
    REPORT_DIR = "reports"

    report_dir = create_directory(directory=REPORT_DIR)
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    zip_files = retrieve_zip_file_name(os.path.join(os.getcwd(), "data"))
    dfs = read_data_into_spark(zip_file_paths=zip_files, sc=spark)

    try:
        for spark_df, dataset_name in zip(dfs.values(), dfs.keys()):
            answering_service = AnsweringService(spark_df=spark_df, dataset_name=dataset_name, spark=spark)
            answering_service.question_one_solution(f"{report_dir}/question1.csv")
            answering_service.question_two_solution(f"{report_dir}/question2.csv")
            answering_service.question_three_solution(f"{report_dir}/question3.csv")
            answering_service.question_four_solution(f"{report_dir}/question4.csv")
            answering_service.question_five_solution(f"{report_dir}/question5.csv")
            answering_service.question_six_solution(f"{report_dir}/question6.csv")
    except Exception as e:
        logger.error(f"An error has occured: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
