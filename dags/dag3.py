from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # Add other default args as needed
}

def query_to_s3(table_name, mysql_conn_id='sql_rewards', s3_bucket=None, aws_conn_id='aws_default'):
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    df = mysql_hook.get_pandas_df(f"SELECT * FROM `{table_name}`")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_key = f'raw/{table_name}.parquet'
    s3_hook.load_bytes(parquet_buffer.getvalue(), bucket_name=s3_bucket, key=s3_key, replace=True)

with DAG('my_dynamic_dag2', default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    
    # Assuming get_table_names() retrieves a list of table names dynamically
    table_names = get_table_names()

    for table_name in table_names:
        query_task = PythonOperator(
            task_id=f'query_{table_name}',
            python_callable=query_to_s3,
            op_kwargs={'table_name': table_name, 's3_bucket': Variable.get("s3_bucket")},
            dag=dag
        )

        sensor_task = S3KeySensor(
            task_id=f'check_s3_{table_name}',
            bucket_key=f'raw/{table_name}.parquet',
            bucket_name=Variable.get("s3_bucket"),
            aws_conn_id='aws_default',
            poke_interval=30,  # Time in seconds between checks
            dag=dag
        )

        query_task >> sensor_task