#!/usr/bin/env python
# coding: utf-8

# In[19]:


# from prefect import flow, task
# import pandas as pd
# from sqlalchemy import create_engine, inspect
# from google.cloud import bigquery
# from google.oauth2 import service_account
# from google.cloud.bigquery import SchemaField
# import os
# import pytz

# # --- Your custom type mapping function ---
# def map_postgres_type_to_bq(postgres_type, column_name=None):
#     type_mapping = {
#         "integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER",
#         "numeric": "NUMERIC", "decimal": "NUMERIC",
#         "real": "FLOAT", "double precision": "FLOAT",
#         "text": "STRING", "character varying": "STRING", "varchar": "STRING",
#         "char": "STRING", "character": "STRING",
#         "timestamp with time zone": "TIMESTAMP", "timestamp without time zone": "TIMESTAMP",
#         "date": "DATE", "time with time zone": "TIME", "time without time zone": "TIME",
#         "boolean": "BOOLEAN", "bytea": "BYTES", "json": "STRING", "jsonb": "STRING"
#     }

#     # Specific overrides
#     if column_name in ['amount', 'refunded_amount']:
#         return "NUMERIC"
#     if column_name == 'company_name':
#         return "STRING"

#     return type_mapping.get(postgres_type.lower(), "STRING")

# # --- Task to get PostgreSQL column types ---
# @task
# def get_pg_column_types(table_name, conn_str):
#     engine = create_engine(conn_str)
#     inspector = inspect(engine)
#     columns = inspector.get_columns(table_name)
#     col_types = {col['name']: str(col['type']) for col in columns}
#     return col_types

# # --- Task to extract data from PostgreSQL ---
# @task
# def extract_data(query, conn_str):
#     engine = create_engine(conn_str)
#     df = pd.read_sql(query, engine)
#     return df

# # --- Helper to build BigQuery schema ---
# def build_bq_schema(df, pg_column_types):
#     schema = []
#     for col in df.columns:
#         pg_type = pg_column_types.get(col, None)
#         bq_type = map_postgres_type_to_bq(pg_type, column_name=col)
#         schema.append(SchemaField(name=col, field_type=bq_type, mode="NULLABLE"))
#     return schema

# # --- WORKING function to fix datetime columns ---
# def fix_datetime_columns(df):
#     """
#     Aggressively fix datetime columns to work with BigQuery
#     """
#     df_copy = df.copy()
    
#     # Find all datetime columns
#     datetime_cols = []
#     for col in df_copy.columns:
#         if 'datetime64' in str(df_copy[col].dtype) or 'timestamp' in str(df_copy[col].dtype).lower():
#             datetime_cols.append(col)
    
#     print(f"Found datetime columns: {datetime_cols}")
    
#     for col in datetime_cols:
#         print(f"Processing datetime column: {col}")
#         print(f"Original dtype: {df_copy[col].dtype}")
        
#         # Convert to string first, then back to datetime (removes all timezone info)
#         df_copy[col] = df_copy[col].astype(str)
#         df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
        
#         # Ensure it's timezone-naive
#         if hasattr(df_copy[col].dtype, 'tz') and df_copy[col].dtype.tz is not None:
#             df_copy[col] = df_copy[col].dt.tz_localize(None)
        
#         print(f"Final dtype: {df_copy[col].dtype}")
    
#     return df_copy

# # --- Task to load data into BigQuery ---
# @task
# def load_to_bigquery(df, table_id, bq_schema):
#     print(f"Original DataFrame dtypes:")
#     print(df.dtypes)
    
#     # Fix datetime columns
#     df_fixed = fix_datetime_columns(df)
    
#     print(f"DataFrame dtypes after datetime fix:")
#     print(df_fixed.dtypes)
    
#     # Handle other data types
#     for col in df_fixed.columns:
#         col_bq_type = None
#         for schema_field in bq_schema:
#             if schema_field.name == col:
#                 col_bq_type = schema_field.field_type
#                 break
        
#         if df_fixed[col].dtype == 'object' and col_bq_type != 'TIMESTAMP':
#             if col_bq_type == 'BOOLEAN':
#                 df_fixed[col] = df_fixed[col].map({
#                     'True': True, 'true': True, 'TRUE': True, True: True,
#                     'False': False, 'false': False, 'FALSE': False, False: False,
#                     'None': None, None: None, 'null': None, 'NULL': None
#                 })
#             else:
#                 df_fixed[col] = df_fixed[col].astype(str)
#                 df_fixed[col] = df_fixed[col].replace('None', None)
    
#     # Load credentials
#     credentials_path = '/Users/abdullahajmal/Abdullah@Ajar/BigQueryUpload/pg-bigquery-pipeline.json'
#     credentials = service_account.Credentials.from_service_account_file(credentials_path)
#     client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#     # Use auto-detect schema instead of manual schema
#     job_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#         autodetect=True,  # Let BigQuery figure out the schema
#         ignore_unknown_values=True,
#         allow_quoted_newlines=True,
#         allow_jagged_rows=True,
#     )

#     try:
#         load_job = client.load_table_from_dataframe(df_fixed, table_id, job_config=job_config)
#         load_job.result()
#         print(f"Loaded {load_job.output_rows} rows to {table_id}")
#     except Exception as e:
#         print(f"Error loading data to BigQuery: {e}")
#         print(f"DataFrame dtypes: {df_fixed.dtypes}")
#         print(f"DataFrame shape: {df_fixed.shape}")
        
#         # Try one more time with string conversion for datetime columns
#         print("Attempting fallback: converting datetime columns to strings...")
#         for col in df_fixed.columns:
#             if 'datetime64' in str(df_fixed[col].dtype):
#                 df_fixed[col] = df_fixed[col].astype(str)
#                 print(f"Converted {col} to string")
        
#         try:
#             load_job = client.load_table_from_dataframe(df_fixed, table_id, job_config=job_config)
#             load_job.result()
#             print(f"Loaded {load_job.output_rows} rows to {table_id} (with string datetime conversion)")
#         except Exception as e2:
#             print(f"Final error: {e2}")
#             raise

# # --- Main Prefect flow ---
# @flow
# def nightly_pg_to_bq():
#     pg_conn_str = "postgresql+psycopg2://tech:>aRSIeB(C,gHuo1|@34.18.1.152/ajar"
#     queries = {
#         "raw_accounts.customer_accounts": """
#             SELECT * FROM accounts
#             WHERE (updated_at + interval '3 hours') >= date_trunc('day', current_date - interval '7 days')
#             AND (updated_at + interval '3 hours') < date_trunc('day', current_date + interval '1 day');
#         """,
#         "payments_data.yesterday_payments": """
#             SELECT * FROM payments
#             WHERE (updated_at + interval '3 hours') >= date_trunc('day', current_date - interval '7 days')
#             AND (updated_at + interval '3 hours') < date_trunc('day', current_date + interval '1 day');
#         """
#     }

#     bq_to_pg_table_map = {
#         "raw_accounts.customer_accounts": "accounts",
#         "payments_data.yesterday_payments": "payments"
#     }

#     for bq_table, sql in queries.items():
#         print(f"Processing table: {bq_table}")
        
#         pg_table_name = bq_to_pg_table_map[bq_table]
#         pg_col_types = get_pg_column_types(pg_table_name, pg_conn_str)
#         df = extract_data(sql, pg_conn_str)
        
#         print(f"Extracted {len(df)} rows from {pg_table_name}")
        
#         bq_schema = build_bq_schema(df, pg_col_types)
#         load_to_bigquery(df, f"ajar-kw.{bq_table}", bq_schema)

# if __name__ == "__main__":
#     nightly_pg_to_bq()


# In[3]:


from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine, inspect
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField
import os
import pytz
import os

from dotenv import load_dotenv
load_dotenv(dotenv_path="/Users/abdullahajmal/Desktop/prefect-pg-bq-pipeline.env")


# Now fetch the secrets
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
print(f"Original path: {credentials_path}")
print(f"Absolute path: {os.path.abspath(credentials_path)}")
print(f"Expanded path: {os.path.expanduser(credentials_path)}")
print(f"File exists: {os.path.exists(credentials_path)}")
print(f"Current working directory: {os.getcwd()}")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine, inspect
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField
import os
import pytz

# --- Your custom type mapping function ---
def map_postgres_type_to_bq(postgres_type, column_name=None):
    type_mapping = {
        "integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER",
        "numeric": "NUMERIC", "decimal": "NUMERIC",
        "real": "FLOAT", "double precision": "FLOAT",
        "text": "STRING", "character varying": "STRING", "varchar": "STRING",
        "char": "STRING", "character": "STRING",
        "timestamp with time zone": "TIMESTAMP", "timestamp without time zone": "TIMESTAMP",
        "date": "DATE", "time with time zone": "TIME", "time without time zone": "TIME",
        "boolean": "BOOLEAN", "bytea": "BYTES", "json": "STRING", "jsonb": "STRING"
    }

    # Specific overrides
    if column_name in ['amount', 'refunded_amount']:
        return "NUMERIC"
    if column_name == 'company_name':
        return "STRING"

    return type_mapping.get(postgres_type.lower(), "STRING")

# --- Task to get PostgreSQL column types ---
@task
def get_pg_column_types(table_name, conn_str):
    engine = create_engine(conn_str)
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    col_types = {col['name']: str(col['type']) for col in columns}
    return col_types

# --- Task to extract data from PostgreSQL ---
@task
def extract_data(query, conn_str):
    engine = create_engine(conn_str)
    df = pd.read_sql(query, engine)
    return df

# --- Helper to build BigQuery schema ---
def build_bq_schema(df, pg_column_types):
    schema = []
    for col in df.columns:
        pg_type = pg_column_types.get(col, None)
        bq_type = map_postgres_type_to_bq(pg_type, column_name=col)
        schema.append(SchemaField(name=col, field_type=bq_type, mode="NULLABLE"))
    return schema

# --- WORKING function to fix datetime columns ---
def fix_datetime_columns(df):
    """
    Aggressively fix datetime columns to work with BigQuery
    """
    df_copy = df.copy()
    
    # Find all datetime columns
    datetime_cols = []
    for col in df_copy.columns:
        if 'datetime64' in str(df_copy[col].dtype) or 'timestamp' in str(df_copy[col].dtype).lower():
            datetime_cols.append(col)
    
    print(f"Found datetime columns: {datetime_cols}")
    
    for col in datetime_cols:
        print(f"Processing datetime column: {col}")
        print(f"Original dtype: {df_copy[col].dtype}")
        
        # Convert to string first, then back to datetime (removes all timezone info)
        df_copy[col] = df_copy[col].astype(str)
        df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
        
        # Ensure it's timezone-naive
        if hasattr(df_copy[col].dtype, 'tz') and df_copy[col].dtype.tz is not None:
            df_copy[col] = df_copy[col].dt.tz_localize(None)
        
        print(f"Final dtype: {df_copy[col].dtype}")
    
    return df_copy

# --- Task to load data into BigQuery ---
@task
def load_to_bigquery(df, table_id, bq_schema):
    print(f"Original DataFrame dtypes:")
    print(df.dtypes)
    
    # Fix datetime columns
    df_fixed = fix_datetime_columns(df)
    
    print(f"DataFrame dtypes after datetime fix:")
    print(df_fixed.dtypes)
    
    # Handle other data types
    for col in df_fixed.columns:
        col_bq_type = None
        for schema_field in bq_schema:
            if schema_field.name == col:
                col_bq_type = schema_field.field_type
                break
        
        if df_fixed[col].dtype == 'object' and col_bq_type != 'TIMESTAMP':
            if col_bq_type == 'BOOLEAN':
                df_fixed[col] = df_fixed[col].map({
                    'True': True, 'true': True, 'TRUE': True, True: True,
                    'False': False, 'false': False, 'FALSE': False, False: False,
                    'None': None, None: None, 'null': None, 'NULL': None
                })
            else:
                df_fixed[col] = df_fixed[col].astype(str)
                df_fixed[col] = df_fixed[col].replace('None', None)
    
    # Load credentials
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print("Credential path being used:", credentials_path)
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Use auto-detect schema instead of manual schema
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,  # Let BigQuery figure out the schema
        ignore_unknown_values=True,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
    )

    try:
        load_job = client.load_table_from_dataframe(df_fixed, table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded {load_job.output_rows} rows to {table_id}")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
        print(f"DataFrame dtypes: {df_fixed.dtypes}")
        print(f"DataFrame shape: {df_fixed.shape}")
        
        # Try one more time with string conversion for datetime columns
        print("Attempting fallback: converting datetime columns to strings...")
        for col in df_fixed.columns:
            if 'datetime64' in str(df_fixed[col].dtype):
                df_fixed[col] = df_fixed[col].astype(str)
                print(f"Converted {col} to string")
        
        try:
            load_job = client.load_table_from_dataframe(df_fixed, table_id, job_config=job_config)
            load_job.result()
            print(f"Loaded {load_job.output_rows} rows to {table_id} (with string datetime conversion)")
        except Exception as e2:
            print(f"Final error: {e2}")
            raise

# --- Main Prefect flow ---
@flow
def nightly_pg_to_bq():
    pg_conn_str = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"
    queries = {
        "raw_accounts.customer_accounts": """
            SELECT * FROM accounts
            WHERE (updated_at + interval '3 hours') >= date_trunc('day', current_date - interval '7 days')
            AND (updated_at + interval '3 hours') < date_trunc('day', current_date + interval '1 day');
        """,
        "payments_data.yesterday_payments": """
            SELECT * FROM payments
            WHERE (updated_at + interval '3 hours') >= date_trunc('day', current_date - interval '7 days')
            AND (updated_at + interval '3 hours') < date_trunc('day', current_date + interval '1 day');
        """
    }

    bq_to_pg_table_map = {
        "raw_accounts.customer_accounts": "accounts",
        "payments_data.yesterday_payments": "payments"
    }

    for bq_table, sql in queries.items():
        print(f"Processing table: {bq_table}")
        
        pg_table_name = bq_to_pg_table_map[bq_table]
        pg_col_types = get_pg_column_types(pg_table_name, pg_conn_str)
        df = extract_data(sql, pg_conn_str)
        
        print(f"Extracted {len(df)} rows from {pg_table_name}")
        
        bq_schema = build_bq_schema(df, pg_col_types)
        load_to_bigquery(df, f"ajar-kw.{bq_table}", bq_schema)

if __name__ == "__main__":
    nightly_pg_to_bq()

