from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import polars as pl
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow import Dataset
from airflow.providers.mysql.operators.mysql import MySqlOperator
import pandas as pd

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
    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    return [table[0] for table in cursor.fetchall()]

with DAG(
    dag_id='sql_to_s3_dag',
    default_args=default_args,
    schedule_interval=None,  # Adjust as needed
    catchup=False,
    tags=['example'],
) as dag:
    tables = get_table_names()
    for table in tables:
        sql_to_s3_task = SqlToS3Operator(
            task_id=f'sql_to_s3_{table}',
            sql_conn_id='sql_rewards',
            query=f'SELECT * FROM `{table}`',
            s3_bucket=Variable.get('s3_bucket'),
            s3_key=f'raw/{table}.parquet',
            replace=True,
            file_format='parquet',
            aws_conn_id='aws_conn_id'
        )

dag =  get_table_names()