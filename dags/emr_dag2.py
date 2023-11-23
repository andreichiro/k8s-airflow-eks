from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Variable
from airflow.decorators.sensor import sensor_task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.hooks.spark_sql import SparkSqlHook
import polars as pl
from tempfile import NamedTemporaryFile
import os
from airflow.utils.task_group import TaskGroup

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
    tables = mysql_hook.get_records('SHOW TABLES;')
    table_names = [table[0] for table in tables]  # Adjust based on the structure of the returned data
    return table_names

@task
def generate_s3_keys(table_names):
    return [f'raw/{table_name}.parquet' for table_name in table_names]


@task
def process_and_upload_to_s3(table_name_xcom, s3_key_xcom):
    s3_bucket = Variable.get("s3_bucket")
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')
    
    # Resolving XComArg objects
    table_name = table_name_xcom.resolve()
    s3_key = s3_key_xcom.resolve()
    
    sql = f"SELECT * FROM `{table_name}`"
    pandas_df = mysql_hook.get_pandas_df(sql)
    polars_df = pl.from_pandas(pandas_df)

    with NamedTemporaryFile() as tmp_file:
        polars_df.write_parquet(tmp_file.name)
        s3_parquet_path = f"{s3_bucket}/{s3_key}"
        s3_hook.load_file(filename=tmp_file.name, key=s3_parquet_path, bucket_name=s3_bucket, replace=True)

with DAG('sql_to_s3_to_emr_serverless', default_args=default_args, schedule_interval='@once', catchup=False, description='DAG to transfer data from MySQL to S3 and trigger an EMR Serverless Spark job') as dag:
    table_names = get_table_names()
    
    with TaskGroup("upload_to_s3_group") as upload_to_s3_group:
        s3_keys = generate_s3_keys(table_names)
        for table_name, s3_key in zip(table_names, s3_keys):
            process_and_upload_to_s3(table_name, s3_key)
        
