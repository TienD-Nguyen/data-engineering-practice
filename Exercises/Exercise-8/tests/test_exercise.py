import sys
sys.path.append("P:\\Projects\\data-engineering-practice\\Exercises\\Exercise-8")

import pytest
import duckdb
from main import create_table_from_csv, question_one_solution
from dataclasses import dataclass, field
import csv
import pandas as pd
from pandas.testing import assert_frame_equal

@dataclass
class SchemaDefinition:
    path: str
    table_name: str
    schema: dict
    kwargs: dict = field(default_factory=dict)

@pytest.fixture(scope="session")
def duck_connection():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()

@pytest.fixture
def sample_schema_definition(tmp_path):
    csv_file = tmp_path / "test_data.csv"
    csv_content = [
        ["VIN", "County", "City", "State", "Model Year"],
        ["1234567890", "King", "Seattle", "WA", "2022"],
        ["534676523", "Pierce", "Tacoma", "WA", "2021"],
        ["1356457434", "Queen", "Seattle", "WA", "2019"]
    ]

    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_content)

    return SchemaDefinition(path=str(csv_file),
                            table_name="electric_vehicle_population",
                            schema={"vin": "VARCHAR(10)",
                                    "country": "VARCHAR(10)",
                                    "city": "VARCHAR(100)",
                                    "state": "VARCHAR(2)",
                                    "model_year": "INTEGER"},
                            kwargs={"header": True})


class TestCreateTableFromCSV:
    def test_create_table_from_csv(self, duck_connection, sample_schema_definition):
        create_table_from_csv(schema_def=sample_schema_definition, connection=duck_connection, drop_if_exist=True)

        table_name = sample_schema_definition.table_name
        result = duck_connection.sql(f"select * from {table_name}").fetchall()
        
        assert result == [("1234567890", "King", "Seattle", "WA", 2022),
                          ("534676523", "Pierce", "Tacoma", "WA", 2021),
                          ("1356457434", "Queen", "Seattle", "WA", 2019)]

class TestQuestionOneSolution:
    @pytest.mark.parametrize("expected_df", [pd.DataFrame({"city": ["Seattle", "Tacoma"], "cnt": [2,1]})])
    def test_question_one_solution(self, duck_connection, expected_df):
        result_df = duck_connection.sql(question_one_solution()).to_df()
        assert_frame_equal(result_df, expected_df)
