from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import Variable
from airflow.decorators.sensor import sensor_task


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

    # Task to transfer data from MySQL to S3
    query_to_s3 = SqlToS3Operator(
        task_id='mysql_to_s3',
        sql_conn_id="sql_rewards",
        aws_conn_id="aws_conn_id",
        query=Variable.get("sql_query"),
        s3_bucket=Variable.get("s3_bucket"),
        s3_key='raw_output.sql',
        replace=True  # Overwrites the S3 file if it exists
    )

    # Task to trigger the EMR Serverless Spark job
    @task
    def trigger_emr_serverless_spark_job():
        aws_hook = AwsBaseHook(Variable.get("aws_conn_id"), client_type='emr-serverless')
        client = aws_hook.get_client_type('emr-serverless')
        job_run_request = {
            'ExecutionRoleArn': Variable.get("emr_execution_role_arn"),
            'ReleaseLabel': 'emr-6.3.0',  # specify the EMR release
            'JobDriver': {
                'SparkSubmit': {
                    'EntryPoint': Variable.get("notebook_s3_path"),  # S3 path to your Jupyter notebook
                    'EntryPointArguments': [],  # any necessary arguments
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

    trigger_emr = trigger_emr_serverless_spark_job()

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

    # Defining the task sequence
    query_to_s3 >> trigger_emr >> emr_serverless_sensor

# Create the DAG instance
dag = sql_to_s3_to_emr_serverless_dag()
