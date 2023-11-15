from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('mysql_connection_test',
         default_args=default_args,
         schedule_interval='@once',  # This DAG will run only once
         catchup=False) as dag:

    test_query = MySqlOperator(
        task_id='test_mysql_connection',
        mysql_conn_id='testing',  # Connection ID from Airflow's Connections
        sql='SELECT 1;'
    )

test_query
