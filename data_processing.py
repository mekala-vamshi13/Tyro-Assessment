import os
import gzip
import logging
import requests
import pandas as pd
import sqlite3
from tqdm import tqdm

# Constants
URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
CHUNK_SIZE = 100000
DB_PATH = "data.db"
TABLE_NAME = "processed_data"
CSV_GZ_FILE = "Tyroo-dummy-data.csv.gz"
SCHEMA_SQL_FILE = "schema.sql"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/process.log"),
        logging.StreamHandler()
    ]
)

def download_file(url, filename):
    try:
        logging.info(f"Downloading from: {url}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1048576):
                    f.write(chunk)
        logging.info("Download complete.")
    except Exception as e:
        logging.error(f"Download failed: {e}")
        raise

def clean_transform(df):
    try:
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        df = df.dropna(how='all')
        df = df.fillna("")
        return df
    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        raise

def generate_schema_sql(df, file_path):
    try:
        column_types = {
            "object": "TEXT",
            "int64": "INTEGER",
            "float64": "REAL",
            "bool": "BOOLEAN"
        }

        columns_sql = []
        for col, dtype in df.dtypes.items():
            sql_type = column_types.get(str(dtype), "TEXT")
            columns_sql.append(f"    {col} {sql_type}")

        create_stmt = f"DROP TABLE IF EXISTS {TABLE_NAME};\n\n"
        create_stmt += f"CREATE TABLE {TABLE_NAME} (\n" + ",\n".join(columns_sql) + "\n);"

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(create_stmt)

        logging.info(f"Schema exported to {file_path}")
    except Exception as e:
        logging.error(f"Failed to generate schema SQL: {e}")
        raise

def insert_chunk(df, connection, if_first_chunk):
    try:
        df.to_sql(TABLE_NAME, connection, if_exists="replace" if if_first_chunk else "append", index=False)
    except Exception as e:
        logging.error(f"Failed to insert chunk: {e}")
        raise

def process_csv(file_path, db_path):
    try:
        conn = sqlite3.connect(db_path)
        with gzip.open(file_path, 'rt', encoding='utf-8', errors='replace') as f:
            logging.info(f"Processing file: {file_path}")
            reader = pd.read_csv(f, chunksize=CHUNK_SIZE)

            for i, chunk in enumerate(tqdm(reader, desc="Processing chunks")):
                cleaned = clean_transform(chunk)

                # Write schema.sql from the first chunk
                if i == 0:
                    generate_schema_sql(cleaned, SCHEMA_SQL_FILE)

                insert_chunk(cleaned, conn, if_first_chunk=(i == 0))
        conn.close()
        logging.info("Processing complete.")
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    try:
        download_file(URL, CSV_GZ_FILE)
        process_csv(CSV_GZ_FILE, DB_PATH)
    except Exception as main_e:
        logging.critical(f"Fatal error: {main_e}")
