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
def upload_tables_to_s3(table_names, s3_keys):
    s3_bucket = Variable.get("s3_bucket")
    spark = SparkSession.builder.appName("ProcessTable").getOrCreate()            
    with SparkSqlHook(mysql_conn_id='sql_rewards') as spark_sql_hook:
        for table_name, s3_key in zip(table_names, s3_keys):
            sql = f"SELECT * FROM {table_name}"
            df = spark_sql_hook.run_query(sql)     
            
             # Directly writing to S3
            s3_parquet_path = f"s3://{s3_bucket}/{s3_key}"
            df.write.parquet(s3_parquet_path)
            
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

@dag(
'sql_to_s3_to_emr_serverless', 
default_args=default_args, 
schedule_interval='@once', 
catchup=False,
description='DAG to transfer data from MySQL to S3 and trigger an EMR Serverless Spark job'
)

def sql_to_s3_to_emr_serverless_dag():
    table_names_list = get_table_names() 
    s3_keys = generate_s3_keys(table_names_list)
    upload_to_s3 = upload_tables_to_s3(table_names_list, s3_keys)

    upload_to_s3 

dag = sql_to_s3_to_emr_serverless_dag()
