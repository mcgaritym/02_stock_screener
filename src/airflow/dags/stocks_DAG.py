# import libraries
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import timedelta
import scripts.stocks_extract_s3, scripts.stocks_extract_rds, scripts.stocks_transform_rds, scripts.stocks_load_query
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
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'stock_screener',
    default_args=default_args,
    description='stock_screener DAG test',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)


# stocks_extract_s3 = BashOperator(
#     task_id='stocks_extract_s3',
#     bash_command='python 02_stocks_extract_s3.py',
#     dag=dag
# )
#
# stocks_extract_rds = BashOperator(
#     task_id='stocks_extract_rds',
#     bash_command='python 02_stocks_extract_rds.py',
#     dag=dag
# )
#
# stocks_transform_rds = BashOperator(
#     task_id='stocks_transform_rds',
#     bash_command='python 02_stocks_transform_rds.py',
#     dag=dag
# )
#
#
# stocks_load_query = BashOperator(
#     task_id='stocks_load_query',
#     bash_command='python 02_stocks_load_query.py',
#     dag=dag
# )

stocks_extract_s3 = PythonOperator(
    task_id='stocks_extract_s3',
    python_callable=scripts.stocks_extract_s3.main,
    dag=dag
)

stocks_extract_rds = PythonOperator(
    task_id='stocks_extract_rds',
    python_callable=scripts.stocks_extract_rds.main,
    dag=dag
)

stocks_transform_rds = PythonOperator(
    task_id='stocks_transform_rds',
    python_callable=scripts.stocks_transform_rds.main,
    dag=dag
)


stocks_load_query = PythonOperator(
    task_id='stocks_load_query',
    python_callable=scripts.stocks_load_query.main,
    dag=dag
)


stocks_extract_s3 >> stocks_extract_rds >> stocks_transform_rds >> stocks_load_query