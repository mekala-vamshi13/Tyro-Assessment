# Tyro-Assessment
Tyroo Data Processing Pipeline
📝 Overview
This Python script performs efficient processing of a large compressed CSV dataset. This includes downloading, decompressing, cleaning, transforming, and storing the data into a SQLite database using chunking to handle large files without memory overload.

📦 Features
Programmatically downloads .csv.gz file from a provided URL
Efficiently reads and processes data using Pandas in chunks
Cleans and transforms the data (whitespace trimming, fill NAs, etc.)
Stores transformed data into a SQL database (SQLite)
Exports the database schema to a .sql file
Handles failures gracefully with error logging
⚙️ Tech Stack
Python 3.8+
Pandas
SQLite3
Requests
Gzip
TQDM
Execution Instructions
Run the script using: python data_processing.py
