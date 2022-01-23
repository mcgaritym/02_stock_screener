from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator


# import python functions in local python files
from sql_connect import sql_connect
from get_tickers import get_tickers
from get_financials import get_financials
from query_stocks import query_stocks
from email_results import email_results

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

    # print date bash task to kickoff events
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # connect to SQL python task
    sql_connect = PythonOperator(
        task_id='sql_connect_',
        python_callable=sql_connect,
        dag=dag,
    )

    # get tickers python task
    get_tickers = PythonOperator(
        task_id='get_tickers_',
        python_callable=get_tickers,
        dag=dag,
    )

    # get financials python task
    get_financials = PythonOperator(
        task_id='get_financials_',
        python_callable=get_financials,
        dag=dag,
    )

    # query stocks python task
    query_stocks = PythonOperator(
        task_id='query_stocks_',
        python_callable=query_stocks,
        dag=dag,
    )



    email_results = PythonOperator(
        task_id='email_results_',
        python_callable=email_results,
        op_kwargs={"sender": 'pythonemail4u@gmail.com',
                   "receiver": ['mcgaritym@gmail.com'],
                   "email_subject": 'Undervalued Stock Picks for Today'},
        dag=dag,
    )



    # specify order/dependency of tasks
    print_date >> sql_connect >> get_tickers >> get_financials >> query_stocks >> email_results
    # print_date >> email_results
