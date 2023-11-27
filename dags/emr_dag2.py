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
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    return tables
tables = get_table_names()

for table in tables:
    sql_to_s3_task = SqlToS3Operator(
            task_id=f"sql_to_s3_{table}",
            sql_conn_id='sql_rewards',
            query=f"SELECT * FROM `{table}`",
            s3_bucket=Variable.get("s3_bucket"),
            s3_key=f'raw/{table}.parquet',
            replace=True,
            file_format='parquet',
            aws_conn_id='aws_conn_id'  # Or your specific AWS connection ID
        )
    
@task
def query_to_s3(table_name):
    s3_bucket = Variable.get("s3_bucket")
    s3_key = f'raw/{table_name}.parquet'
    sql_operator = SqlToS3Operator(
        task_id=f"sql_to_s3_{table_name}",
        sql_conn_id='sql_rewards',
        query=f"SELECT * FROM `{table_name}`",
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
        file_format='parquet'
    )
        
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
    query_to_s3 = query_to_s3.expand(table_name=tables)

tables >> query_to_s3