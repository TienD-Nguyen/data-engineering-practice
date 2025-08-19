import pytest
import tempfile
import os
import zipfile
from io import BytesIO
from pyspark.sql import SparkSession
import shutil

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def temp_directory():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def sample_csv_content():
    return """date,serial_number,model,capacity_bytes,failure
            2023-01-01,SN001,Samsung SSD 980,1000000000000,0
            2023-01-02,SN002,WD Blue SSD,500000000000,1
            2023-01-03,SN003,Seagate Barracuda,2000000000000,0"""

@pytest.fixture
def test_zip_file(temp_directory, sample_csv_content):
    zip_path = os.path.join(temp_directory, "test_data_2025_08_17.zip")
    with zipfile.ZipFile(zip_path, "w") as zip_ref:
        zip_ref.writestr("data.csv", sample_csv_content)
    return zip_path

@pytest.fixture
def empty_zip_file(temp_directory):
    zip_path = os.path.join(temp_directory, "empty.zip")
    with zipfile.ZipFile(zip_path, "w") as zip_ref:
        pass
    return zip_path

@pytest.fixture
def zip_with_macos_files(temp_directory, sample_csv_content):
    zip_path = os.path.join(temp_directory, "macos_test.zip")
    with zipfile.ZipFile(zip_path, "w") as zip_ref:
        zip_ref.writestr("data.csv", sample_csv_content)
        zip_ref.writestr("__MACOSX/._data.csv", "system file content")
        zip_ref.writestr(".DS_Store", "system file content")
    return zip_path
