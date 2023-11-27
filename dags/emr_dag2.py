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
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    tables = cursor.fetchall()

    for table in tables:  
        df = mysql_hook.get_pandas_df(f"SELECT * FROM `{table}`")
#        parquet_buffer = io.BytesIO()
        df.to_parquet(index=False)
         
def query_to_s3():
    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    tables = cursor.fetchall()
    sql = f"SELECT * FROM `{table}`"
    s3_key = f'raw/{table}.parquet'
    s3_bucket = Variable.get("s3_bucket")
    
    for table in tables:
        df = mysql_hook.get_pandas_df(f"SELECT * FROM `{table}`")
        sql_operator = SqlToS3Operator(
        task_id=f"sql_to_s3_{table}",
        sql_conn_id='sql_rewards',
        query=sql,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
        file_format='parquet'  # Assuming you want to save the data in Parquet format
    )
        return df

        
#@task
#def generate_s3_keys(table_names):
#    """
#    Task to generate S3 keys for storing Parquet files.
#    """
#    files_paths = [f'raw/{table_name}.parquet' for table_name in table_names]
#    return files_paths###


#@task
#def create_sql_to_s3_task(table_name):
#    """
#    Task to create and execute SqlToS3Operator for a specific table.
#    """
#    return SqlToS3Operator(
#        task_id=f"sql_to_s3_{table_name}",
#        sql_conn_id='sql_rewards',  # Replace with your actual connection ID
#        query=f"SELECT * FROM `{table_name}`",
#        s3_bucket=Variable.get("s3_bucket"),
#        s3_key=f'raw/{table_name}.parquet',
#        replace=True
#    )

# Define the main DAG
with DAG(
    'my_dynamic_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set the interval as needed
    catchup=False,
    tags=['example'],
) as dag:
    tables = get_table_names()
    query_to_s3 = query_to_s3()
#    query_to_s3 = create_sql_to_s3_task.expand(table_name=tables)

    
    # Task 1: Get table names from MySQL
#    table_names_task = get_table_names()
#    for table in table_names_task:
#        print(table)
        
    # Task 2: Generate S3 keys
 #   s3_keys_task = generate_s3_keys(table_names_task)
 #   for s3 in s3_keys_task:
 #       print(s3)
    
      # Create and execute SqlToS3Operator tasks for each table
#    sql_to_s3_tasks = []

#    for table_name in table_names_task:
#        s3_key = f'raw/{table_name}.parquet'
#        sql_to_s3_task = create_sql_to_s3_task(table_name, s3_key)
#        sql_to_s3_tasks.append(sql_to_s3_task)
        
    # Set up dependencies
[tables, query_to_s3]