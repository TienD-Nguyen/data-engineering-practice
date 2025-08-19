import sys
sys.path.append("P:\Projects\data-engineering-practice\Exercises\Exercise-7")

import pytest
import zipfile
from io import BytesIO
from unittest.mock import patch, MagicMock
import os
from main import read_zip_content_into_memory, create_spark_dataframe_from_memory

class TestReadZipContentIntoMemory:
    def test_read_csv_from_zio(self, test_zip_file):
        result = read_zip_content_into_memory(test_zip_file)
        assert len(result) == 1
        assert isinstance(result[0], BytesIO)

        content = result[0].getvalue().decode('utf-8')
        assert "date,serial_number,model" in content
        assert "Samsung SSD 980" in content

    def test_skip_macos_system_files(self, zip_with_macos_files):
        result = read_zip_content_into_memory(zip_with_macos_files)
        assert len(result) == 1
        content = result[0].getvalue().decode('utf-8')

        assert "Samsung SSD 980" in content
        assert "__MACOSX" not in content
        assert ".DS_Store" not in content

    def test_empty_zip_file(self, empty_zip_file):
        result = read_zip_content_into_memory(empty_zip_file)
        assert result == []

    def test_zip_with_multiple_csv_files(self, temp_directory):
        zip_path = os.path.join(temp_directory, "multi_csv.zip")
        
        with zipfile.ZipFile(zip_path, "w") as zip_ref:
            zip_ref.writestr("data1.csv", "col1,col2\nval1,val2")
            zip_ref.writestr("data2.csv", "col3,col4\nval3,val4")

        result = read_zip_content_into_memory(zip_path)

        assert len(result) == 2
        content = result[0].getvalue().decode("utf-8")
        assert "col1,col2" in content or "col3,col4" in content

class TestCreateSparkDataframeFromMemory:
    def test_create_data_frame_with_header(self, spark_session, sample_csv_content):
        csv_io = BytesIO(sample_csv_content.encode("utf-8"))

        df = create_spark_dataframe_from_memory(csv_io, spark_session, header=True, infer_schema=True)

        assert df.count() == 3
        columns = df.columns
        expected_columns = ["date", "serial_number", "model", "capacity_bytes", "failure"]
        assert all(col in columns for col in expected_columns)

    def test_create_data_frame_without_header(self, spark_session, sample_csv_content):
        csv_io = BytesIO(sample_csv_content.encode('utf-8'))
        
        df = create_spark_dataframe_from_memory(csv_io, spark_session, header=False, infer_schema=True)

        columns = df.columns
        assert "_c0" in columns
        assert df.count() == 4

    def test_create_dataframe_schema_inference(self, spark_session):
        csv_content = "int_col,str_col,float_col\n1,hello,3.14\n2,world,2.71"
        csv_io = BytesIO(csv_content.encode('utf-8'))

        df = create_spark_dataframe_from_memory(csv_io, spark_session, header=True, infer_schema=True)

        schema = df.schema
        type_map = {field.name: field.dataType.simpleString() for field in schema.fields}

        assert 'int' in type_map['int_col']
        assert 'string' in type_map['str_col']
        assert 'double' in type_map['float_col']

    def test_empty_csv_content(self, spark_session):
        csv_io = BytesIO(b"")

        df = create_spark_dataframe_from_memory(csv_io, spark_session, header=True, infer_schema=True)

        assert df.isEmpty() == True
        assert df.columns == []