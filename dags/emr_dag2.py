from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import polars as pl
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

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
def create_sql_to_s3_tasks(table_names, s3_keys, sql_conn_id, s3_bucket):
    for table_name, s3_key in zip(table_names, s3_keys):
        SqlToS3Operator(
            task_id=f"sql_to_s3_{table_name}",
            sql_conn_id=sql_conn_id,
            query=f"SELECT * FROM `{table_name}`",
            s3_bucket=s3_bucket,
            s3_key=s3_key,
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

    create_sql_to_s3_tasks(table_names_task, s3_keys_task, 'sql_rewards', Variable.get("s3_bucket"))
  
    #sql_to_s3_tasks = [create_sql_to_s3_task(table, s3_key) for table, s3_key in zip(table_names_task, s3_keys_task)]
