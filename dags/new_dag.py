from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.models import Variable
from datetime import datetime, timedelta
import functools
from airflow.operators.subdag import SubDagOperator

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

def get_table_names(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='sql_rewards')
    tables = mysql_hook.get_records('SHOW TABLES;')
    table_names = [table[0] for table in tables]
    kwargs['ti'].xcom_push(key='table_names', value=table_names)

def branch_tasks(table_names, **kwargs):
    return ['sql_to_s3_' + table for table in table_names]

def create_sql_to_s3_subdag(parent_dag_name, child_dag_name, table_name, args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@daily",
    )

    with dag_subdag:
        SqlToS3Operator(
            task_id=f"sql_to_s3_{table_name}",
            sql_conn_id='sql_rewards',
            query=f"SELECT * FROM `{table_name}`",
            s3_bucket=Variable.get("s3_bucket"),
            s3_key=f'raw/{table_name}.parquet',
            replace=True,
            dag=dag_subdag
        )
    
    return dag_subdag

with DAG('my_dynamic_dag', default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    
    get_tables = PythonOperator(
        task_id='get_table_names',
        python_callable=get_table_names,
        provide_context=True
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=branch_tasks,
        provide_context=True,
        op_kwargs={'table_names': "{{ ti.xcom_pull(task_ids='get_table_names', key='table_names') }}"}
    )

    get_tables >> branching

    # Create tasks dynamically
    for table in get_tables.output:
        subdag_task = SubDagOperator(
            task_id=f'sql_to_s3_{table}',
            subdag=create_sql_to_s3_subdag('my_dynamic_dag', f'sql_to_s3_{table}', table, default_args),
            default_args=default_args,
            dag=dag,
        )
        branching >> subdag_task
