from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# import python functions in local python files
from sql_connect import sql_connect_1
from get_tickers_2 import get_tickers_2
from get_financials import get_financials
from query_stocks import query_stocks

import pymysql

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mcgaritym@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'stocks_dag',
    default_args=default_args,
    description='Stock Screener DAG, delivering undervalued stock recommendations',
    # schedule_interval="@hourly",
    schedule_interval=None,
    start_date=datetime(2021, 1, 9),
    catchup=False,
    tags=['stock_dag_tag'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # t1.doc_md = dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    #
    # """
    # )

    # define the first task
    t2 = PythonOperator(
        task_id='sql_connect_',
        python_callable=sql_connect_1,
        dag=dag,
    )

    # define the first task
    t3 = PythonOperator(
        task_id='get_tickers_2_',
        python_callable=get_tickers_2,
        dag=dag,
    )

    # t3 = BashOperator(
    #     task_id='get_tickers_',
    #     bash_command='python /Users/mcgaritym/airflow-docker/dags/get_tickers.py',
    #     dag=dag
    #
    # )
    # define the first task
    t4 = PythonOperator(
        task_id='get_financials_',
        python_callable=get_financials,
        dag=dag,
    )

    # define the first task
    t5 = PythonOperator(
        task_id='query_stocks_',
        python_callable=query_stocks,
        dag=dag,
    )

    # specify order/dependency of tasks
    # t1 >> t2 >> t3 >> t4 >> t5
    t3 >> t4 >> t5
