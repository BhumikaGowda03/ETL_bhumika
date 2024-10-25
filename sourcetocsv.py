import oracledb
import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

# Load environment variables
load_dotenv()

# Initialize the Oracle client with the provided path
oracledb.init_oracle_client(lib_dir=r"C:\Users\bhumika.gowda\Downloads\instantclient-basic-windows.x64-23.5.0.24.07\instantclient_23_5")

# Define Oracle connection credentials
username = os.getenv('oracle_username')
password = os.getenv('password')
dsn = os.getenv('dsn')  # Oracle host, port, and service name

# Create a SQLAlchemy engine
oracle_connection_string = f"oracle+oracledb://{username}:{password}@{dsn}"
engine = create_engine(oracle_connection_string)

# Initialize S3 client
s3_client = boto3.client('s3')
bucket_name = 'etlbucketbhumika'

# Define the schema name
schema_name = os.getenv('schema').upper()
schema_folder = schema_name.lower()  # Use schema name as folder name in lowercase


try:
    # Query to get all table names in the schema
    tables_query = f"SELECT table_name FROM all_tables WHERE owner = '{schema_name}'"
    tables_df = pd.read_sql(tables_query, con=engine)


    # Iterate over each table
    for table in tables_df['table_name']:
        try:
            # Retrieve column names for the table
            columns_query = f"SELECT column_name FROM all_tab_columns WHERE table_name = '{table}' AND owner = '{schema_name}'"
            columns_df = pd.read_sql(columns_query, con=engine)
            columns = ", ".join(columns_df['column_name'].tolist()) 


            # Fetch table data
            table_query = f"SELECT {columns} FROM {schema_name}.{table}"
            df = pd.read_sql(table_query, con=engine)
            
            # Convert DataFrame to CSV format in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Define the S3 file path with schema folder
            file_name = f"{schema_folder}/{table}.csv"  # schema_folder/table.csv
            
            # Upload the CSV data directly to S3 under schema folder
            s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
            print(f"Table '{table}' has been saved to '{file_name}' in the '{bucket_name}' bucket.")
        except Exception as e:
            print(f"Failed to upload table '{table}' to S3: {e}")

except Exception as e:
    print(f"Failed to retrieve tables or save data to S3: {e}")
finally:
    # Close the connection
    engine.dispose()
