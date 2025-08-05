import psycopg2
import os

def log_info(message):
    print(f"[INFO] - {message}")
    
def log_error(message):
    print(f"[ERROR] - {message}")

def generate_account_ddl(table_name="account", database_type="postgresql"):
    ddl = f"""-- {table_name.upper()} Table DDL Script
    -- Database: {database_type.upper()}
    CREATE TABLE IF NOT EXISTS {table_name} (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        address_1 VARCHAR(255),
        address_2 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        zip_code VARCHAR(20),
        join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX idx_{table_name}_customer_id ON {table_name}(customer_id);
    """
    return ddl.strip()

def generate_product_ddl(table_name="product", database_type="postgresql"):
    ddl = f"""-- {table_name.upper()} Table DDL Script
    -- Database: {database_type.upper()}
    CREATE TABLE IF NOT EXISTS {table_name} (
        product_id SERIAL PRIMARY KEY,
        product_code VARCHAR(50) NOT NULL UNIQUE,
        product_description TEXT
    );
    CREATE INDEX idx_{table_name}_product_id ON {table_name}(product_id);
    """
    return ddl.strip()

def generate_transaction_ddl(table_name="transaction", database_type="postgresql"):
    ddl = f"""-- {table_name.upper()} Table DDL Script
    -- Database: {database_type.upper()}
    CREATE TABLE IF NOT EXISTS {table_name} (
        transaction_id TEXT PRIMARY KEY,
        transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        product_id INTEGER NOT NULL,
        product_code VARCHAR(50) NOT NULL,
        product_description TEXT,
        quantity INTEGER DEFAULT 1,
        account_id INTEGER NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
    );
    
    CREATE INDEX idx_{table_name}_transaction_id ON {table_name}(transaction_id);
    """
    return ddl.strip()

tables = {
    "accounts": generate_account_ddl,
    "products": generate_product_ddl,
    "transactions": generate_transaction_ddl
}

def drop_table(table_name: str):
    return f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    """
    
def copy_from_csv(file_name: str, table_name: str, cursor: psycopg2.extensions.cursor):
    with open(file_name, 'r') as f:
        next(f)
        cursor.copy_from(f, table_name, sep=",")

def main():
    host = "postgres"
    database = "exercise_database"
    user = "exercise_user"
    pas = "postgres"
    
    try:
        log_info("Connecting to the database...")
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        cur = conn.cursor()
        
        log_info("Executing DDL scripts and copying data from CSV files...")
    
        for csv_file in os.listdir("data"):
            table_name = csv_file.removesuffix(".csv")
            cur.execute(drop_table(table_name))
            table_ddl = tables[table_name](table_name=table_name)
            cur.execute(table_ddl)
            copy_from_csv(f"data/{csv_file}", table_name, cur)
            print(f"Data from {csv_file} copied to {table_name} table.")
    except Exception as e:
        log_error(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.commit()
            conn.close()
        log_info("Database connection closed.")

if __name__ == "__main__":
    main()
