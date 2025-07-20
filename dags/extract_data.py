import logging
import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
from google.cloud import storage
from sqlalchemy import create_engine
from datetime import timedelta,datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Load environment variables
# load_dotenv('/opt/airflow/.env')

# Configure logging to output to the console
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gideon/Library/CloudStorage/OneDrive-Personal/MyLab/Data_Engineering/Amdari_Projects/maxi-project/single_infra/servicekey.json'

# Retrieve environment variables
postgres_user = os.getenv('POSTGRES_USER')
postgres_db = os.getenv('POSTGRES_DB')
postgres_host = os.getenv('POSTGRES_HOST')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_port=os.getenv('POSTGRES_PORT')
bucket_name = os.getenv('BUCKET_NAME')

tables = ['customers','inventory','products','sale_transactions','sales_managers','stores']

default_args = {
    'owner': 'Ytech',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def connect_to_db():
    try:
        # Format: 'postgresql+psycopg2://user:password@host:port/dbname'   
        db_url = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        engine = create_engine(db_url)
        logging.info("Connected to PostgreSQL!!!")
        return engine
    except Exception as e:
        logging.error(f"Eror connecting to PostgreSQL database: {e}")
        raise

def extract_data(engine, qry):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(qry,conn.connection)
            return df
    except Exception as e:
        logging.error(f"Error executing query: {qry}. Error: {e}")
        return None
    
def format_data(df, filepath):
    try:
        df.to_parquet(filepath,index=False)
        return filepath
    except Exception as e:
        logging.error(f"Error formatting data {e}")
        return None
    
def upload_to_gcs(source_filename, bucket_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"sales_data/{destination_blob_name}")
        blob.upload_from_filename(source_filename)
        logging.info(f"File {source_filename} uploaded to {destination_blob_name}.")
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {e}")
        raise

def validate_data_quality(table_name: str, engine):
    try:
        # Define primary key column for each table
        primary_keys = {
            'stores': 'store_id',
            'sale_transactions': 'transaction_id',
            'inventory': 'inventory_id',
            'customers': 'customer_id',
            'products': 'product_id',
            'sales_managers': 'manager_id'
        }

        pk_column = primary_keys.get(table_name, 'id')  # Default to 'id' if not found
        
        # Basic validation queries
        validations = {
            'row_count': f"SELECT COUNT(*) FROM {table_name}",
            'null_check': f"SELECT COUNT(*) FROM {table_name} WHERE {pk_column} IS NULL",
            'duplicate_check': f"SELECT COUNT(*) - COUNT(DISTINCT {pk_column}) FROM {table_name}"
        }

        results = {}
        with engine.connect() as conn:
            for validation_name, query in validations.items():
                result = pd.read_sql(query, conn.connection).iloc[0, 0]
                results[validation_name] = result
                logging.info(f"{table_name} - {validation_name}: {result}")

        # Define validation rules
        validation_passed = True
        
        # Check if table has data
        if results['row_count'] == 0:
            logging.error(f"{table_name}: Table is empty")
            validation_passed = False

        # Check for null primary keys (assuming 'id' column exists)
        if results['null_check'] > 0:
            logging.warning(f"{table_name}: Found {results['null_check']} null primary keys")

        # Check for duplicates
        if results['duplicate_check'] > 0:
            logging.warning(f"{table_name}: Found {results['duplicate_check']} duplicate records")

        if validation_passed:
            logging.info(f"{table_name}: Data quality validation passed")
            return {"status": "passed", "table": table_name, "metrics": results}
        else:
            raise ValueError(f"{table_name}: Data quality validation failed")
            
    except Exception as e:
        logging.error(f"Validation failed for {table_name}: {str(e)}")
        raise


# Main function to run the entire pipeline
def extract_data_pipeline(table_name:str, **kwargs):
    query = f"SELECT * FROM {table_name};"
    
    # Connect to PostgreSQL
    engine = connect_to_db()
    try:
        # Extract data from the table
        df = extract_data(engine, query)

        if df is not None:
            logging.info(f"Data extracted for table: {table_name}")

            # Format the data to Parquet
            parquet_file = format_data(df, f"{table_name}.parquet")

            if parquet_file:
                # Upload the Parquet file to GCS
                upload_to_gcs(parquet_file, bucket_name, f"{table_name}.parquet")

                # Perform data quality validation
                validate_data_quality(table_name, engine)
        else:
            logging.warning(f"No data extracted for table: {table_name}")
    except Exception as e:
        logging.error(f"Error {e}")
    finally:
        engine.dispose()
        logging.info("Database engine disposed")

with DAG(
    'extract_sales_data_pipeline',
    default_args=default_args,
    description='Extract the Sales Data into GCP',
    schedule='0 0 * * *',
    start_date=datetime(2025,7,20),
    catchup=False,
    tags=['etl','sales', 'extract', 'production']
) as dag:
    for table in tables:
        extract_data_task = PythonOperator(
            task_id=f'extract_{table}_data',
            python_callable=extract_data_pipeline,
            op_kwargs={'table_name': table},  # pass table name to function
        )



