from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import polars as pl
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
    print(table_names)
    return table_names

@task
def generate_s3_keys(table_names):
    """
    Task to generate S3 keys for storing Parquet files.
    """
    files_paths = []
    for table_name in table_names:
        files_paths.append(f'raw/{table_name}.parquet')
    print(files_paths)
    return files_paths

@task
def process_and_upload_to_s3(table_names, files_paths):
    """
    Task to process data and upload it to S3.
    """
    s3_bucket = Variable.get("s3_bucket")
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')

    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')

    for table_name, path in zip(table_names, files_paths):
        sql = f"SELECT * FROM `{table_name}`"
        pandas_df = mysql_hook.get_pandas_df(sql)
        polars_df = pl.from_pandas(pandas_df)
        polars_df.write_parquet(path)

# Define the main DAG
with DAG(
    'my_dynamic_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set the interval as needed
    catchup=False,
    tags=['example'],
) as dag:
    # Task 1: Get table names from MySQL
    table_names = get_table_names()

    # Task 2: Generate S3 keys
    keys = generate_s3_keys(table_names)

    # Task 3: Process and upload to S3
    process_and_upload = process_and_upload_to_s3(table_names, keys)

# Define the task dependencies as needed
table_names >> keys >> process_and_upload
