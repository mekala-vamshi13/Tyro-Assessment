

## Tyroo Data Processing Pipeline

###  Overview

This Python script efficiently processes a large compressed CSV dataset. It performs the following steps:

- Downloads the dataset from a given URL  
- Decompresses the `.csv.gz` file  
- Cleans and transforms the data  
- Stores the cleaned data into a SQLite database  
- Uses chunking to handle large files without running out of memory  

---

###  Features

- Automatically downloads a `.csv.gz` file from a specified URL  
- Processes data in chunks using **Pandas** for memory efficiency  
- Cleans and transforms the data:
- Trims whitespace  
- Fills missing values (NAs)  
- Stores the processed data into a **SQLite** database  
-  Exports the database schema to a `.sql` file  
- Logs errors for graceful handling of failures  

---

### Tech Stack

- Python 3.8+  
- Pandas  
- SQLite3  
- Requests  
- Gzip  
- TQDM  

---

### Execution Instructions

To run the script, use the following command:


python data_processing.py
