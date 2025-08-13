import sys
sys.path.append("P:\\Projects\\data-engineering-practice\\Exercises\\Exercise-6")

import pytest
import zipfile
from io import BytesIO
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pathlib import Path
from main import read_zip_content_into_memory, create_spark_dataframe_from_memory, read_data_into_spark, AnsweringService
import os
import shutil

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("pyspark-test").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_zip():
    zip_path = Path.cwd() / "test.zip"
    csv_content = "started_at,value\n2025-01-01,100\n2025-01-02, 200"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("valid.csv", csv_content)
        zf.writestr("__MACOSX/junk", "junk content")
        zf.writestr("folder/._hidden.csv", "shoule be ignored")
        zf.writestr(".DS_Store", "system file")

    yield zip_path

    if zip_path.exists():
        os.remove(zip_path)

def test_read_zip_content_filters_junk(sample_zip):
    files = read_zip_content_into_memory(str(sample_zip))
    assert len(files) == 1
    assert files[0][0] == "valid.csv"
    assert isinstance(files[0][1], BytesIO)

def test_create_spark_dataframe_from_memory(spark, sample_zip):
    files = read_zip_content_into_memory(str(sample_zip))
    name, csv_io = files[0]
    df = create_spark_dataframe_from_memory(csv_io, sc=spark, header=True, infer_schema=True)

    assert "started_at" in df.columns
    assert "value" in df.columns
    data = df.collect()
    assert len(data) == 2
    assert data[0]["value"] == 100

def test_read_data_into_spark_adds_date_column(spark, sample_zip):
    result = read_data_into_spark([str(sample_zip)], sc=spark)
    assert "valid.csv" in result
    df = result["valid.csv"]

    assert "date" in df.columns

    rows = df.select(psf.date_format("date", "yyyy-MM-dd").alias("d")).collect()
    assert rows[0]["d"] == "2025-01-01"

@pytest.fixture
def sample_trip_data(spark):
    data = [
        ("2025-01-01", "S1", "Station One"),
        ("2025-01-02", "S1", "Station One"),
        ("2025-01-03", "S2", "Station Two"),
        ("2025-02-01", "S2", "Station Two"),
        ("2025-02-02", "S2", "Station Two"),
        ("2025-02-03", "S1", "Station One"),
    ]
    df = spark.createDataFrame(data, ["date", "start_station_id", "start_station_name"])\
            .withColumn("data", psf.to_date(psf.col("date")))
    return df

def test_question_three_solution_creates_correct_csv(spark, sample_trip_data):
    output_dir = Path.cwd() / "question_three_output"

    if output_dir.exists():
        shutil.rmtree(output_dir)
    
    service = AnsweringService(sample_trip_data, "test_dataset", spark)
    service.question_three_solution(str(output_dir))

    assert output_dir.exists()

    csv_files = list(output_dir.glob("*.csv")) or list(output_dir.rglob("*.csv"))
    assert csv_files, "No CSV files found in output directory"

    result_df = spark.read.csv(str(output_dir), header=True)

    expected_cols = {"year", "month", "start_location_id", "start_location_name", "trip_count"}
    assert expected_cols.issubset(set(result_df.columns))

    results = {(int(row["year"]), int(row["month"])): row["start_location_id"] for row in result_df.collect()}
    assert results[(2025,1)] == "S1"
    assert results[(2025,2)] == "S2"
    
    shutil.rmtree(output_dir)