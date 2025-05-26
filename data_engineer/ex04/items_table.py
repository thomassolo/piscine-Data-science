import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

# Database connection details
DB_NAME = "piscineds"
DB_USER = "tsoloher"  
DB_PASSWORD = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5432"
CSV_FILE = "../../item/item.csv"

# Step 1: Read CSV file
if not os.path.exists(CSV_FILE):
    print(f"❌ Error: {CSV_FILE} not found!")
    exit(1)

try:
    df = pd.read_csv(CSV_FILE)
    print(f"✅ CSV file loaded: {df.shape[0]} rows, {df.shape[1]} columns")
except Exception as e:
    print(f"❌ Error reading CSV file: {e}")
    exit(1)

# Step 2: Define PostgreSQL data type mapping
type_mapping = {
    "int64": "INTEGER",
    "float64": "DECIMAL(10,2)",
    "bool": "BOOLEAN",
    "object": "TEXT",
    "datetime64": "TIMESTAMP"
}

# Step 3: Process columns and ensure we have at least 3 different data types
table_name = "item"  # Fixed table name as required
columns = {}

# If the first column appears to be a timestamp, convert it
try:
    first_col = df.columns[0]
    df[first_col] = pd.to_datetime(df[first_col], errors='coerce')
    print("First column converted to timestamp")
except:
    print("First column is not a timestamp")

# Map column types
for col, dtype in df.dtypes.items():
    dtype_str = str(dtype)
    if dtype_str in type_mapping:
        columns[col] = type_mapping[dtype_str]
    else:
        columns[col] = "TEXT"  # Default to TEXT for unknown types

# Ensure we have at least 3 different data types
unique_types = set(columns.values())
if len(unique_types) < 3:
    print("⚠️ Less than 3 data types detected. Enforcing variety...")
    
    # Try to convert some columns based on likely content
    if 'price' in df.columns:
        columns['price'] = 'DECIMAL(10,2)'
        
    if 'in_stock' in df.columns:
        columns['in_stock'] = 'BOOLEAN'
        
    if 'created_at' in df.columns:
        columns['created_at'] = 'TIMESTAMP'

# Step 4: Generate SQL Table Schema
columns_sql = ", ".join([f'"{col}" {dtype}' for col, dtype in columns.items()])
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"

# Step 5: Connect to PostgreSQL and Create Table
try:
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    print(f"✅ Table {table_name} created successfully!")
    
    # Show data types used
    print(f"✅ Data types used: {', '.join(set(columns.values()))}")
except Exception as e:
    print(f"❌ Error creating table: {e}")
    exit(1)  

# Step 6: Insert Data
try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df.to_sql(table_name, engine, if_exists="replace", index=False, chunksize=1000)
    print(f"✅ Data from {CSV_FILE} inserted successfully!")
except Exception as e:
    print(f"❌ Error inserting data: {e}")

# Close connection
conn.close()