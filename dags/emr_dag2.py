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
def process_and_upload_to_s3(table_name, s3_key):
    s3_bucket = Variable.get("s3_bucket")
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')

    with MySqlHook(mysql_conn_id='sql_rewards') as mysql_hook:
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
        process_and_upload_to_s3(table_names, s3_keys)
        
        # Upload to S3
#            with S3Hook(aws_conn_id='aws_conn_id') as s3_hook:
#                s3_hook.load_bytes(
#                    bytes_data=s3_parquet_path,
#                    bucket_name=s3_bucket, 
#                    key=s3_key,
#                    replace=True
#                )
                  
# Task to trigger the EMR Serverless Spark job
#@task
#def trigger_emr_serverless_spark_job(s3_paths):
#    aws_hook = AwsBaseHook(Variable.get("aws_conn_id"), client_type='emr-serverless')
#    client = aws_hook.get_client_type('emr-serverless')
#    job_run_request = {
#        'ExecutionRoleArn': Variable.get("emr_execution_role_arn"),
#        'ReleaseLabel': 'emr-6.3.0',  # specify the EMR release
#        'JobDriver': {
#            'SparkSubmit': {
#                'EntryPoint': Variable.get("notebook_s3_path"),  # S3 path to your Jupyter notebook
#                'EntryPointArguments': s3_paths,  # Pass the S3 paths as arguments
#                'SparkSubmitParameters': '--conf spark.executor.instances=2'  # Spark parameters
#            }
#        },
#        'ConfigurationOverrides': {
#            'ApplicationConfiguration': [],  # additional configurations
#            'MonitoringConfiguration': {}  # monitoring configurations
#        }
#    }
#    response = client.start_job_run(**job_run_request)
#    return response['jobRunId']#

# Sensor task to monitor the EMR Serverless job status
#@sensor_task(timeout=300, mode="poke", poke_interval=30)
#def emr_serverless_sensor(job_run_id):
#    aws_hook = AwsBaseHook(Variable.get("aws_conn_id"), client_type='emr-serverless')
#    client = aws_hook.get_client_type('emr-serverless')
#    response = client.describe_job_run(id=job_run_id)
#    status = response['jobRun']['state']
#    if status == 'SUCCESS':
#        return True
#    elif status in ['FAILED', 'CANCELLED']:
#        raise ValueError('EMR Serverless Job failed or was cancelled')
#    return False

