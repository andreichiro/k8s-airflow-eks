from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import polars as pl
from airflow.models import Variable
# Import the correct operator for SQL to S3 transfer
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySqlToS3Operator

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
    files_paths = [f'raw/{table_name}.parquet' for table_name in table_names]
    return files_paths

@task
def process_and_upload_to_s3(table_name, s3_key):
    """
    Task to process data and upload it to S3.
    """
    s3_bucket = Variable.get("s3_bucket")
    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')
    sql = f"SELECT * FROM `{table_name}`"
    pandas_df = mysql_hook.get_pandas_df(sql)
    polars_df = pl.from_pandas(pandas_df)
    parquet_data = polars_df.to_parquet()

    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    
    # Upload the Parquet data to S3
    s3_hook.load_bytes(
        parquet_data,
        key=f'{s3_bucket}/{s3_key}',
        bucket_name=s3_bucket,
        replace=True
        )
# Define the main DAG
with DAG(
    'my_dynamic_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set the interval as needed
    catchup=False,
    tags=['example'],
) as dag:
   
    # Task 1: Get table names from MySQL
    table_names_task = get_table_names()

    # Task 2: Generate S3 keys
    s3_keys_task = generate_s3_keys(table_names_task)


    @task_group(group_id='sql_to_s3_group')
    def process_tables_to_s3(tables, s3_keys):
        for table, s3_key in zip(tables, s3_keys):
            sql_to_s3_task = MySqlToS3Operator(
                task_id=f"sql_to_s3_{table}",
                sql_conn_id='your_actual_sql_connection_id',  # Replace with your actual connection ID
                query=f"SELECT * FROM `{table}`",
                s3_bucket=Variable.get("s3_bucket"),
                s3_key=s3_key,
                replace=True
            )
            
    # Instantiate the task group and set up dependencies
   # Instantiate the task group and set up dependencies
    process_group = process_tables_to_s3(table_names_task, s3_keys_task)
    table_names_task >> s3_keys_task >> process_group