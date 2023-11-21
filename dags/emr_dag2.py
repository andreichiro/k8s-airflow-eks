from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Variable
from airflow.decorators.sensor import sensor_task
from airflow.providers.mysql.hooks.mysql import MySqlHook

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

@dag(
'sql_to_s3_to_emr_serverless', 
default_args=default_args, 
schedule_interval='@once', 
catchup=False,
description='DAG to transfer data from MySQL to S3 and trigger an EMR Serverless Spark job'
)
def sql_to_s3_to_emr_serverless_dag():
    table_names = get_table_names()
    s3_paths = []

    for table_name in table_names:
        s3_path = f's3://{Variable.get("s3_bucket")}/{s3_key}'
        s3_key = f'raw/{table_name}.sql'
        s3_paths.append(s3_path)

        SqlToS3Operator(
            task_id=f'mysql_to_s3_{table_name}',
            sql_conn_id="sql_rewards",
            aws_conn_id="aws_conn_id",
            query=f"SELECT * FROM {table_name}",
            s3_bucket=Variable.get("s3_bucket"),
            s3_key=s3_key,
            replace=True
        )
    return s3_paths
        
# Task to trigger the EMR Serverless Spark job
@task
def trigger_emr_serverless_spark_job(s3_paths):
    aws_hook = AwsBaseHook(Variable.get("aws_conn_id"), client_type='emr-serverless')
    client = aws_hook.get_client_type('emr-serverless')
    job_run_request = {
        'ExecutionRoleArn': Variable.get("emr_execution_role_arn"),
        'ReleaseLabel': 'emr-6.3.0',  # specify the EMR release
        'JobDriver': {
            'SparkSubmit': {
                'EntryPoint': Variable.get("notebook_s3_path"),  # S3 path to your Jupyter notebook
                'EntryPointArguments': s3_paths,  # Pass the S3 paths as arguments
                'SparkSubmitParameters': '--conf spark.executor.instances=2'  # Spark parameters
            }
        },
        'ConfigurationOverrides': {
            'ApplicationConfiguration': [],  # additional configurations
            'MonitoringConfiguration': {}  # monitoring configurations
        }
    }
    response = client.start_job_run(**job_run_request)
    return response['jobRunId']

# Sensor task to monitor the EMR Serverless job status
@sensor_task(timeout=300, mode="poke", poke_interval=30)
def emr_serverless_sensor(job_run_id):
    aws_hook = AwsBaseHook(Variable.get("aws_conn_id"), client_type='emr-serverless')
    client = aws_hook.get_client_type('emr-serverless')
    response = client.describe_job_run(id=job_run_id)
    status = response['jobRun']['state']
    if status == 'SUCCESS':
        return True
    elif status in ['FAILED', 'CANCELLED']:
        raise ValueError('EMR Serverless Job failed or was cancelled')
    return False

@dag(
    'sql_to_s3_to_emr_serverless', 
    default_args=default_args, 
    schedule_interval='@once', 
    catchup=False,
    description='DAG to transfer data from MySQL to S3 and trigger an EMR Serverless Spark job'
)

def sql_to_s3_to_emr_serverless_dag():
    """
    DAG for transferring data from MySQL to S3 and then triggering an EMR Serverless Spark job.
    """

    table_names = get_table_names()
    s3_paths = sql_to_s3_to_emr_serverless_dag(table_names)
    # Call the task function to create a task instance
    trigger_emr_instance = trigger_emr_serverless_spark_job(s3_paths)

    # Call the sensor task function to create a task instance
    emr_serverless_sensor_instance = emr_serverless_sensor(trigger_emr_instance)

    # Defining the task sequence
        # Define task dependencies
    s3_paths >> trigger_emr_instance >> emr_serverless_sensor_instance

# Create the DAG instance
dag = sql_to_s3_to_emr_serverless_dag()
