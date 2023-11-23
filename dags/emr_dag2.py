from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import polars as pl
from tempfile import NamedTemporaryFile
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # Add other default args as needed
}

@task
def get_table_names():
    """
    Task to retrieve table names from MySQL database.
    """
    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')
    tables = mysql_hook.get_records('SHOW TABLES;')
    table_names = [table[0] for table in tables]  # Adjust based on the structure of the returned data
    return table_names

@task
def generate_s3_keys(table_name):
    """
    Task to generate S3 keys for storing Parquet files.
    """
    return f'raw/{table_name}.parquet'
    
@task
def process_and_upload_to_s3(table_name, s3_key):
    """
    Task to process data and upload it to S3.
    """
    s3_bucket = Variable.get("s3_bucket")
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
   
    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')
    sql = f"SELECT * FROM `{table_name}`"
    pandas_df = mysql_hook.get_pandas_df(sql)
    polars_df = pl.from_pandas(pandas_df)

    with NamedTemporaryFile() as tmp_file:
        polars_df.write_parquet(tmp_file.name)
        s3_parquet_path = f"{s3_bucket}/{s3_key}"
        s3_hook.load_file(filename=tmp_file.name, key=s3_parquet_path, bucket_name=s3_bucket, replace=True)

# Define the DAG
with DAG('sql_to_s3_to_emr_serverless', default_args=default_args, schedule_interval=None, catchup=False, description='DAG to transfer data from MySQL to S3 and trigger an EMR Serverless Spark job') as dag:
    
    # Task 1: Get table names from MySQL
    table_names = get_table_names()
    
    with TaskGroup("upload_to_s3_group") as upload_to_s3_group:
        # Task 2: Generate S3 keys for storing Parquet files
        s3_keys = generate_s3_keys(table_names)
        
        # Task 3: Process and upload data to S3 in parallel
        process_and_upload_to_s3(table_names, s3_keys)

