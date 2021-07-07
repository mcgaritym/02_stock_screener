# import libraries
from datetime import timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# specify arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mcgaritym@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_demo',
    default_args=default_args,
    description='stock_screener DAG test',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)
