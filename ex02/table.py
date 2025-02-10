import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Database connection details
DB_NAME = "piscineds"
DB_USER = "tsoloher"  
DB_PASSWORD = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5432"
CSV_FILE = "customer/data_2022_oct.csv"  

# Step 1: Read CSV file
df = pd.read_csv(CSV_FILE)

# Step 2: Define PostgreSQL data type mapping
type_mapping = {
    "int64": "INTEGER",
    "float64": "DECIMAL(10,2)",
    "bool": "BOOLEAN",
    "object": "TEXT",
    "datetime64": "TIMESTAMP"
}

# Convert first column to TIMESTAMP
df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])

# Step 3: Generate SQL Table Schema
table_name = CSV_FILE.split("/")[-1].replace(".csv", "")  
columns = df.dtypes.apply(lambda x: type_mapping[str(x)]).to_dict()
columns[list(df.columns)[0]] = "TIMESTAMP NOT NULL"  # Ensure first column is TIMESTAMP
columns_sql = ", ".join([f'"{col}" {dtype}' for col, dtype in columns.items()])
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"

# Step 4: Connect to PostgreSQL and Create Table
try:
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Table {table_name} created successfully!")
except Exception as e:
    print(f"❌ Error creating table: {e}")
    exit(1)  

# Step 5: Insert Data in Batches
try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    with engine.connect() as conn:
        print("✅ Connected to database successfully!")
    df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=1000)
    print(f"✅ Data from {CSV_FILE} inserted successfully!")
except Exception as e:
    print(f"❌ Error inserting data: {e}")
