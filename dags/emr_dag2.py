from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Variable
from airflow.decorators.sensor import sensor_task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import EmrCreateNotebookExecutionOperator


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
def trigger_emr_notebook(table_name, sql_query):
    notebook_execution_name = f'notebook_execution_{table_name}'
    notebook_name = 'notebook'  # Specify the name of your EMR Notebook
    notebook_params = {
        'table_name': table_name,
        'sql_query': sql_query
    }
    # Use EmrCreateNotebookExecutionOperator to trigger the EMR Notebook (Spark) here
    emr_notebook_task = EmrCreateNotebookExecutionOperator(
        task_id=notebook_execution_name,
        aws_conn_id='aws_conn_id',  # Specify your AWS connection ID
        notebook_execution_name=notebook_execution_name,
        notebook_name=notebook_name,
        notebook_params=notebook_params,
        do_xcom_push=True,
        )
    return emr_notebook_task

@task
def upload_tables_to_s3(table_names, s3_keys):
    s3_bucket = Variable.get("s3_bucket")

    with MySqlHook(mysql_conn_id='sql_rewards') as mysql_hook:
        for table_name, s3_key in zip(table_names, s3_keys):
            sql = f"SELECT * FROM {table_name}"
            emr_notebook_task = trigger_emr_notebook(table_name, sql)
            spark_data = emr_notebook_task.output  # Retrieve output from the EMR Notebook
            
            # Upload to S3
            with S3Hook(aws_conn_id='aws_conn_id') as s3_hook:
                s3_hook.load_bytes(
                    bytes_data=spark_data,
                    bucket_name=s3_bucket, 
                    key=s3_key,
                    replace=True
                )
                  
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
    table_names_list = get_table_names() 
    s3_keys = generate_s3_keys(table_names_list)
    upload_to_s3 = upload_tables_to_s3(table_names_list, s3_keys)

    trigger_emr_instance = trigger_emr_serverless_spark_job(s3_keys)
    emr_serverless_sensor_instance = emr_serverless_sensor(trigger_emr_instance)

    upload_to_s3 >> trigger_emr_instance >> emr_serverless_sensor_instance

dag = sql_to_s3_to_emr_serverless_dag()
