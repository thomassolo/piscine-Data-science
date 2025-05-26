import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import glob

# Database connection details
DB_NAME = "piscineds"
DB_USER = "tsoloher"  
DB_PASSWORD = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5432"
CUSTOMER_FOLDER = "../../customer/" 

# Step 1: Read CSV file
csv_files = glob.glob(os.path.join(CUSTOMER_FOLDER, "*.csv"))

# Step 2: Define PostgreSQL data type mapping
type_mapping = {
    "int64": "INTEGER",
    "float64": "DECIMAL(10,2)",
    "bool": "BOOLEAN",
    "object": "TEXT",
    "datetime64": "TIMESTAMP"
}

# Connect to PostgreSQL
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

# Process each CSV file
for csv_file in csv_files:
    print(f"Processing {csv_file}...")
    
    try:
        # Step 1: Read CSV file
        print("  Reading CSV file...")
        df = pd.read_csv(csv_file)
        print(f"  CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Convert first column to TIMESTAMP
        print("  Converting timestamp column...")
        try:
            df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], errors='coerce')
            # Drop rows where timestamp conversion failed
            df = df.dropna(subset=[df.columns[0]])
            print("  Timestamp conversion complete")
        except Exception as e:
            print(f"  Error converting timestamps: {e}")
            continue
        
        # Step 3: Generate SQL Table Schema
        table_name = os.path.basename(csv_file).replace(".csv", "").lower()
        print(f"  Generating schema for table '{table_name}'...")
        
        # Handle any data type that might not be in the mapping
        columns = {}
        for col, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            if dtype_str in type_mapping:
                columns[col] = type_mapping[dtype_str]
            else:
                columns[col] = "TEXT"  # Default to TEXT for unknown types
                
        columns[list(df.columns)[0]] = "TIMESTAMP NOT NULL"  # Ensure first column is TIMESTAMP
        columns_sql = ", ".join([f'"{col}" {dtype}' for col, dtype in columns.items()])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        
        # Step 4: Create Table and insert data
        print("  Creating table in database...")
        try:
            cur = conn.cursor()
            cur.execute(create_table_query)
            conn.commit()
            
            print("  Inserting data (this may take a while for large files)...")
            # Use SQLAlchemy to load data into the table
            engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
            
            # Use chunking for large files
            if len(df) > 100000:
                print("  Large file detected, using chunked processing...")
                chunk_size = 10000
                for i in range(0, len(df), chunk_size):
                    df.iloc[i:i+chunk_size].to_sql(table_name, engine, if_exists='append' if i > 0 else 'replace', index=False)
                    print(f"  Processed {min(i+chunk_size, len(df))}/{len(df)} rows")
            else:
                df.to_sql(table_name, engine, if_exists='replace', index=False)
            
            print(f"Successfully processed {table_name}")
        except Exception as e:
            print(f"Error creating table or inserting data: {e}")
            conn.rollback()
            
    except Exception as e:
        print(f"Error processing {csv_file}: {e}")
        continue

# Close connection
conn.close()