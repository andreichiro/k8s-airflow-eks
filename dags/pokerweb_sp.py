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
from airflow.providers.mysql.operators.mysql import SQLExecuteQueryOperator 

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

def get_tables():
    """
    Task to retrieve table names from MySQL database.
    """
    
    mysql_hook = MySqlHook(mysql_conn_id="sql_pokerweb_sp")
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute('SHOW TABLES;')
    tables = [table[0] for table in cursor.fetchall()]     
    return tables

def sanitize_task_id(table_name):
    return (
        table_name.replace(' ', '_')
                  .replace('.', '_')
                  .replace('(', '_')
                  .replace(')', '_')
                  .replace(',', '_')
                  .replace('-', '_')  # Replace dashes as well 
    )
                
def sql_to_s3(table):
    """
    Task to generate S3 keys for storing Parquet files.
    """    
    sql_to_s3_task = SqlToS3Operator(
                task_id=f"sql_to_s3_{sanitize_task_id(table)}",
                sql_conn_id='sql_bi_rewards',
                query=f"SELECT * FROM `{table}`",
                s3_bucket=Variable.get("s3_bucket"),
                s3_key=f'bi_rewards/raw/{table}.parquet',
                replace=True,
                file_format='parquet',
                aws_conn_id='aws_conn_id'  # Or your specific AWS connection ID
        )
     
with DAG(
    'pokerweb_sp_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set the interval as needed
    catchup=False,
    tags=['example'],
) as dag:
    tables = get_tables()
    for table in tables:
        sanitized_table = sanitize_task_id(table)
        sql = sql_to_s3(table=sanitized_table)
#    query_to_s3 = query_to_s3.expand(table_name=tables)

