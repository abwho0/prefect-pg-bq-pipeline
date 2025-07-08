import psycopg2
import csv
import subprocess # Temporarily keep if you must use gsutil, but we'll remove it soon
from google.cloud import storage, bigquery
from google.oauth2 import service_account # Only needed if using a service account key file. Best to use default CF SA.
import os
from datetime import datetime, timedelta, date
import logging
import io # For in-memory CSV/Parquet handling instead of local files
import pandas as pd # For efficient data handling and Parquet conversion

# --- 1. Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 2. Configuration (These will be read from environment variables) ---
# DB Connection
# IMPORTANT: These will be set as environment variables in Cloud Functions
# during deployment, often mapped from Secret Manager for passwords.
DB_HOST = os.environ.get('POSTGRES_HOST')
DB_NAME = os.environ.get('POSTGRES_DB')
DB_USER = os.environ.get('POSTGRES_USER')
DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
DB_PORT = os.environ.get('POSTGRES_PORT', '5432') # Default to 5432 if not set

# Google Cloud Storage
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')

# BigQuery
BIGQUERY_PROJECT_ID = os.environ.get('BIGQUERY_PROJECT_ID')
# BIGQUERY_DATASET_NAME = os.environ.get('BIGQUERY_DATASET_NAME')
# Note: BIGQUERY_SERVICE_ACCOUNT_KEY_PATH is removed. We'll use the
# Cloud Function's default service account directly via IAM roles.

# dbt Cloud API
# DBT_CLOUD_TOKEN = os.environ.get('DBT_CLOUD_TOKEN')
# DBT_ACCOUNT_ID = os.environ.get('DBT_ACCOUNT_ID')
# DBT_JOB_ID = os.environ.get('DBT_JOB_ID')

# --- 3. Helper Functions (Deduplicated and Modified) ---

def connectionNew():
    """Establishes a connection to the PostgreSQL database."""
    try:
        logger.info(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}...")
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        logger.info("Successfully connected to PostgreSQL.")
        return connection
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        # Re-raise to ensure the Cloud Function fails if DB connection fails
        raise

def query_executeNew(connection, query):
    """Executes a query on the given connection and returns the results."""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchall(), cursor.description
    except psycopg2.Error as e:
        logger.error(f"Error executing query: {e}")
        # Re-raise to ensure the Cloud Function fails if query fails
        raise

def map_postgres_type_to_bq(postgres_type, column_name=None):
    """
    Maps PostgreSQL data types to BigQuery data types, with specific overrides.
    Deduplicated and combined logic for 'amount'/'refunded_amount' and 'company_name'.
    """
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

    # Specific overrides based on column name for type consistency in BigQuery
    if column_name in ['amount', 'refunded_amount']:
        return "NUMERIC"
    if column_name == 'company_name':
        return "STRING" # Assuming you want company_name as string

    return type_mapping.get(postgres_type, "STRING") # Default to STRING if not found

# --- 4. MODIFIED: Export and Upload Logic (Combined, Uses Pandas & Google Cloud Storage Client) ---

def export_and_upload_to_gcs(table_name, query_sql, gcs_subfolder):
    """
    Exports data from a specified PostgreSQL table using a given query,
    then uploads it directly to GCS as a Parquet file.
    Returns the GCS path of the uploaded file.
    """
    conn = None
    try:
        conn = connectionNew()
        if conn is None:
            logger.error(f"Failed to connect to PostgreSQL for {table_name} export.")
            raise Exception("DB Connection Failed")

        logger.info(f"Executing query for {table_name}: {query_sql}")
        records, description = query_executeNew(conn, query_sql)
        
        if not records:
            logger.info(f"No records found for {table_name} with the given query.")
            return None, None # Return None if no data

        # Convert records to a Pandas DataFrame
        column_names = [desc[0] for desc in description]
        df = pd.DataFrame(records, columns=column_names)
        logger.info(f"Successfully extracted {len(df)} rows from {table_name}.")

        # Use datetime for filenames, adjusted for Kuwait time (current_date - interval 'X' + 3 hours)
        # This aligns the filename timestamp with your SQL's logic for 'current_date' in Kuwait time.
        # Since Cloud Functions run in UTC, we simulate the +3 hours for naming consistency.
        kuwait_time_now = datetime.utcnow() + timedelta(hours=3) 
        
        # Determine the file naming convention based on your SQL queries
        # For 'accounts', it's based on date_trunc('day', current_date - interval '3 months')
        # For 'payments', it's based on date_trunc('day', current_date - interval '7 days')
        # To simplify, we'll just use a generic run timestamp for the filename.
        timestamp_for_filename = kuwait_time_now.strftime('%Y%m%d_%H%M%S')
        
        filename = f"{table_name}_{timestamp_for_filename}.parquet"
        gcs_file_path = f"gs://{GCS_BUCKET_NAME}/{gcs_subfolder}/{filename}" # e.g., raw/accounts_20250704_170000.parquet

        # Convert DataFrame to Parquet in-memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0) # Rewind to the beginning of the buffer

        # Upload to GCS using google-cloud-storage client
        logger.info(f"Uploading {table_name} data to GCS: {gcs_file_path}")
        storage_client = storage.Client(project=BIGQUERY_PROJECT_ID) # Use default CF SA implicitly
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(f"{gcs_subfolder}/{filename}") # Blob name is path within bucket
        blob.upload_from_file(parquet_buffer, content_type='application/octet-stream') # application/octet-stream for parquet
        
        logger.info(f"Successfully uploaded {table_name} data to {gcs_file_path}")
        
        # Also return the BigQuery schema inferred from the PostgreSQL query result
        # This is more robust than querying information_schema separately
        bq_schema_fields = []
        for col_name, pg_type_code, display_size, internal_size, precision, scale, null_ok in description:
            # psycopg2.extensions.get_type_by_oid(pg_type_code).name gives the type name
            pg_type_name = psycopg2.extensions.get_type_by_oid(pg_type_code).name
            bq_type = map_postgres_type_to_bq(pg_type_name, column_name=col_name)
            bq_mode = 'NULLABLE' if null_ok else 'REQUIRED'
            bq_schema_fields.append(bigquery.SchemaField(col_name, bq_type, mode=bq_mode))
        
        logger.info(f"Inferred BigQuery schema for {table_name}: {[f.name for f in bq_schema_fields]}")
        
        return gcs_file_path, bq_schema_fields

    except Exception as e:
        logger.error(f"Error during export/upload for {table_name}: {str(e)}")
        raise # Re-raise the exception to signal failure
    finally:
        if conn:
            conn.close()

# --- 5. MODIFIED: Load to BigQuery (Uses Parquet, Dynamic Schema) ---

def load_to_bigquery_with_client_library(gcs_path, table_name, bq_schema, dataset_name):
    """
    Loads data from GCS into BigQuery using the BigQuery Python client library.
    """
    logger.info(f"Loading data from {gcs_path} to BigQuery table: {table_name} in dataset: {dataset_name}")
    try:
        full_table_id = f"{BIGQUERY_PROJECT_ID}.{dataset_name}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            schema=bq_schema,
            source_format=bigquery.SourceFormat.PARQUET, # <-- CORRECTED THIS TO PARQUET
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # <-- CHANGE THIS TO WRITE_APPEND
            autodetect=False,
            # skip_leading_rows=1, # This line can be removed as it's for CSV
        )

        uri = gcs_path

        load_job = bq_client.load_table_from_uri(
            uri,
            full_table_id,
            job_config=job_config,
        )

        load_job.result()
        logger.info(f"Load job for {full_table_id} finished.")
        logger.info(f"Loaded {load_job.output_rows} rows into {full_table_id}.")
        return True
    except Exception as e:
        logger.error(f"BigQuery load failed for {table_name} in {dataset_name}: {str(e)}")
        raise

# --- 6. MODIFIED: Delete GCS File (Uses Google Cloud Storage Client) ---

def delete_gcs_file(gcs_file_path):
    """Deletes a file from Google Cloud Storage using the Python client library."""
    if not gcs_file_path:
        logger.info("No GCS file path provided for deletion. Skipping.")
        return True

    try:
        storage_client = storage.Client(project=BIGQUERY_PROJECT_ID)
        # Parse bucket name and blob name from full GCS path
        path_parts = gcs_file_path.replace("gs://", "").split("/", 1)
        bucket_name = path_parts[0]
        blob_name = path_parts[1]

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        logger.info(f"Successfully deleted {gcs_file_path} from GCS.")
        return True
    except Exception as e:
        logger.warning(f"Warning: Error deleting GCS file {gcs_file_path}: {e}")
        return False # Log warning but don't fail the pipeline, as data is already loaded

# --Since dbt developer plan does not have API access

# --- 7. NEW: Trigger dbt Cloud Job ---
# import requests # Need to import requests library

# def trigger_dbt_cloud_job():
#     """
#     Trigger dbt Cloud job via API
#     """
#     if not DBT_CLOUD_TOKEN or not DBT_ACCOUNT_ID or not DBT_JOB_ID:
#         logger.warning("DBT Cloud API credentials/IDs not fully set (DBT_CLOUD_TOKEN, DBT_ACCOUNT_ID, DBT_JOB_ID). Skipping dbt Cloud trigger.")
#         return False

#     url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    
#     headers = {
#         'Authorization': f'Token {DBT_CLOUD_TOKEN}',
#         'Content-Type': 'application/json'
#     }
    
#     data = {
#         'cause': 'Triggered by automated raw data pipeline'
#     }
    
#     logger.info(f"Attempting to trigger dbt Cloud job {DBT_JOB_ID} for account {DBT_ACCOUNT_ID}...")
#     try:
#         response = requests.post(url, headers=headers, json=data)
#         response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        
#         dbt_run_id = response.json().get('data', {}).get('id')
#         logger.info(f"✅ dbt Cloud job triggered successfully! Run ID: {dbt_run_id}")
#         return dbt_run_id
            
#     except requests.exceptions.RequestException as e:
#         logger.error(f"❌ Error triggering dbt Cloud job: {str(e)}")
#         if e.response:
#             logger.error(f"dbt Cloud API Response Status: {e.response.status_code}")
#             logger.error(f"dbt Cloud API Response Content: {e.response.text}")
#         raise # Re-raise to ensure the Cloud Function fails if dbt trigger fails

# --- 8. Main Cloud Function Entry Point ---

def main(request):
    """
    Main Cloud Function entry point.
    Orchestrates the data extraction, upload, BigQuery load, and dbt trigger.
    """
    logger.info("Starting automated data pipeline...")
    
    # Define table-specific parameters
    # Note: Using date_trunc('day', current_date) + interval '3 hours' to align with Kuwait timezone
    # As current_date in SQL is often based on the DB server's timezone.
    # It's generally better to use explicit time zones or UTC in SQL if possible.
    
    # For accounts (updated in last 7 days)
    accounts_query_sql = f"""
        SELECT *
        FROM accounts
        WHERE (updated_at + interval '3 hours') >= date_trunc('day', current_date - interval '7 days')
        AND (updated_at + interval '3 hours') < date_trunc('day', current_date + interval '1 day'); -- Include today's data till 23:59:59 Kuwait time
    """
    
    # For payments (updated in last 7 days)
    payments_query_sql = f"""
        SELECT *
        FROM payments
        WHERE (updated_at + interval '3 hours') >= date_trunc('day', current_date - interval '7 days')
        AND (updated_at + interval '3 hours') < date_trunc('day', current_date + interval '1 day'); -- Include today's data till 23:59:59 Kuwait time
    """

    processed_tables = {}
    
    # --- Process Accounts Data ---
    try:
        logger.info("--- Processing accounts table ---")
        gcs_accounts_path, accounts_bq_schema = export_and_upload_to_gcs(
            table_name='customer_accounts',
            query_sql=accounts_query_sql,
            gcs_subfolder='raw_accounts' # Separate subfolder for accounts
        )
        
        if gcs_accounts_path and accounts_bq_schema:
            load_to_bigquery_with_client_library(gcs_accounts_path, 'customer_accounts', accounts_bq_schema, 'raw_accounts')
            delete_gcs_file(gcs_accounts_path) # Clean up GCS after load
            processed_tables['customer_accounts'] = True
        else:
            logger.info("No new accounts data to process or load.")
            processed_tables['customer_accounts'] = False

    except Exception as e:
        logger.error(f"Accounts pipeline failed: {str(e)}")
        return {'status': 'error', 'message': f"Accounts pipeline failed: {str(e)}"}, 500 # Fail immediately if one pipeline fails

    # --- Process Payments Data ---
    try:
        logger.info("--- Processing payments table ---")
        gcs_payments_path, payments_bq_schema = export_and_upload_to_gcs(
            table_name='yesterday_payments',
            query_sql=payments_query_sql,
            gcs_subfolder='raw_payments' # Separate subfolder for payments
        )

        if gcs_payments_path and payments_bq_schema:
            load_to_bigquery_with_client_library(gcs_payments_path, 'yesterday_payments', payments_bq_schema, 'payments_data')
            delete_gcs_file(gcs_payments_path) # Clean up GCS after load
            processed_tables['yesterday_payments'] = True
        else:
            logger.info("No new payments data to process or load.")
            processed_tables['yesterday_payments'] = False

    except Exception as e:
        logger.error(f"Payments pipeline failed: {str(e)}")
        return {'status': 'error', 'message': f"Payments pipeline failed: {str(e)}"}, 500 # Fail immediately if one pipeline fails

    # --- Trigger dbt Cloud Job (Only if at least one table was processed successfully) ---
    # if processed_tables.get('accounts') or processed_tables.get('payments'):
    #     try:
    #         dbt_run_id = trigger_dbt_cloud_job()
    #         logger.info(f"dbt Cloud job run ID: {dbt_run_id}")
    #     except Exception as e:
    #         logger.error(f"Failed to trigger dbt Cloud job after data load: {str(e)}")
    #         return {'status': 'error', 'message': f"dbt trigger failed: {str(e)}"}, 500
    # else:
    #     logger.info("No data was processed for any table. Skipping dbt Cloud trigger.")

    # logger.info("Overall pipeline completed successfully!")
    # return {
    #     'status': 'success',
    #     'message': 'Pipeline completed successfully',
    #     'processed_tables': processed_tables
    # }

# For local testing (optional: only if you set up local env vars)
if __name__ == "__main__":
    # Set dummy environment variables for local testing (replace with actual values if testing locally)
    os.environ['POSTGRES_HOST'] = 'your_local_pg_host'
    os.environ['POSTGRES_DB'] = 'your_local_pg_db'
    os.environ['POSTGRES_USER'] = 'your_local_pg_user'
    os.environ['POSTGRES_PASSWORD'] = 'your_local_pg_password'
    os.environ['POSTGRES_PORT'] = '5432'
    os.environ['GCS_BUCKET_NAME'] = 'your_gcs_bucket_for_testing'
    os.environ['BIGQUERY_PROJECT_ID'] = 'your_gcp_project_id_for_testing'
    os.environ['BIGQUERY_DATASET_NAME'] = 'your_bq_dataset_for_testing'
    # os.environ['DBT_CLOUD_TOKEN'] = 'your_dbt_token_for_testing'
    # os.environ['DBT_ACCOUNT_ID'] = 'your_dbt_account_id_for_testing'
    # os.environ['DBT_JOB_ID'] = 'your_dbt_job_id_for_testing'

    class MockRequest:
        pass
    
    result = main(MockRequest())
    print(result)