import duckdb
import os
from dataclasses import dataclass, field
import logging
import csv

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger()

@dataclass
class SchemaDefinition:
    path: str
    table_name: str
    schema: dict
    kwargs: dict = field(default_factory=dict)

def define_table_schema(file_path: str):
    schema_def = SchemaDefinition(
        path = os.path.join(os.getcwd(), file_path),
        table_name = "electric_vehicle_population",
        schema = {
            "vin": "VARCHAR(10)",
            "country": "VARCHAR(50)",
            "city": "VARCHAR(100)",
            "state": "VARCHAR(2)",
            "postal_code": "VARCHAR(10)",
            "model_year": "INTEGER",
            "make": "VARCHAR(50)",
            "model": "VARCHAR(100)",
            "electric_vehicle_type": "VARCHAR(50)",
            "cafv_eligibility": "VARCHAR(100)",
            "electric_range": "INTEGER",
            "base_msrp": "DECIMAL(10, 2)",
            "legislative_district": "VARCHAR(10)",
            "dol_vehicle_id": "BIGINT",
            "vehile_location": "VARCHAR(200)",
            "electric_utility": "VARCHAR(100)",
            "census_tract_2020": "VARCHAR(20)"
        },
        kwargs = {"header": True},
    )

    return schema_def

def retrieve_csv_columns(csv_file_path):
    with open(csv_file_path, 'r', newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    return header

def create_table_with_schema(table_schema: dict, table_name: str, connection: duckdb.DuckDBPyConnection):
    columns = ",\n    ".join([f"{col} {dtype}" for col, dtype in table_schema.items()])
    create_table_sql = f"CREATE TABLE {table_name} ({columns});"
    
    try:
        connection.execute(create_table_sql)
        logger.info(f"Table '{table_name}' schema created successfuully with {len(table_schema)} columns.")
    except Exception as e:
        logger.error(f"Error creating table with define schema: {e}")
        raise

def insert_data_from_csv(table_schema: dict, csv_path: str, table_name: str, connection: duckdb.DuckDBPyConnection):
    db_columns = ", ".join(list(table_schema.keys()))
    csv_columns = ", ".join([f'"{col}"' for col in retrieve_csv_columns(csv_path)])

    insert_sql = f"""
    INSERT INTO {table_name} ({db_columns})
    SELECT {csv_columns}
    FROM read_csv('{csv_path}')
    """

    try:
        connection.execute(insert_sql)
        count_result = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        record_count = count_result[0] if count_result else 0
        print(f"Successfully loaded {record_count} records into table '{table_name}' from '{os.path.basename(csv_path)}'")

        connection.sql(f"SELECT * FROM {table_name} LIMIT 10").show()
    except Exception as e:
        logger.error(f"Error loading data from CSV: {e}")
        raise

def create_table_from_csv(schema_def: SchemaDefinition, connection: duckdb.DuckDBPyConnection, drop_if_exist: bool):
    table_schema = schema_def.schema
    csv_path = schema_def.path
    table_name = schema_def.table_name
    
    if drop_if_exist:
        logger.info(f"Checking for table '{table_name}' and delete if exists.")
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    logger.info(f"Creating '{table_name}' with defined schema.")
    create_table_with_schema(table_schema, table_name, connection)
    
    logger.info(f"Inserting csv data into table '{table_name}'.")
    insert_data_from_csv(table_schema, csv_path, table_name, connection)

def question_one_solution():
    return "SELECT city, COUNT(*) AS cnt FROM electric_vehicle_population GROUP BY city"

def question_two_solution():
    return """\
    SELECT model, COUNT(*) AS cnt
    FROM electric_vehicle_population
    GROUP BY model
    ORDER BY cnt DESC
    LIMIT 3
    """

def question_three_solution():
    return """\
    WITH max_count AS (SELECT postal_code,
                              model,
                              COUNT(*) AS vehicle_count
                       FROM electric_vehicle_population
                       GROUP BY postal_code, model)
                       
    SELECT
        postal_code,
        model
    FROM 
    (
        SELECT postal_code,
               model,
               RANK() OVER (PARTITION BY postal_code ORDER BY vehicle_count DESC) AS model_rank
        FROM max_count
    )
    WHERE model_rank = 1"""

def question_four_solution():
    return """
    SELECT model_year,
           COUNT(*) AS number_of_vehicles
    FROM electric_vehicle_population
    GROUP BY model_year"""

def main():
    schema_def = define_table_schema("data/Electric_Vehicle_Population_Data.csv")
    conn = duckdb.connect(":memory:")
    create_table_from_csv(schema_def=schema_def, connection=conn, drop_if_exist=True)

    questions_bank = {"Q1": [question_one_solution(), "Count the number of electric cars per city."],
                      "Q2": [question_two_solution(), "Find the top 3 most popular electric vehicles."],
                      "Q3": [question_three_solution(), "Find the most popular electric vehicle in each postal code."],
                      "Q4": [question_four_solution(), "Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year."]}

    for quest_num, task in questions_bank.items():
        query = task[0]
        quest_req = task[1]
        logger.info(f"{quest_num} - {quest_req}")
        conn.query(query).show()
        if quest_num == "Q4":
            output_file = "data/q4_result.parquet"
            export_sql = f"COPY ({query}) TO '{os.path.join(os.getcwd(), output_file)}' (FORMAT parquet)"
            conn.execute(export_sql)

if __name__ == "__main__":
    main()
