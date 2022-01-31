# import libraries
# import airflow libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# import python functions in local python files
from create_database import create_database
from get_tickers import get_tickers
from get_financials import get_financials
from query_stocks import query_stocks
from email_results import email_results

# specify default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mcgaritym@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=5),
    'dagrun_timeout': timedelta(hours=5),

}

with DAG(
    'stocks_dag',
    default_args=default_args,
    description='Stock Screener DAG, delivering undervalued stock recommendations',
    schedule_interval="@weekly",
    start_date=datetime.now(),
    catchup=False,
    tags=['stock_dag_tag'],
) as dag:

    # print date bash task to kickoff events
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # connect to SQL python task
    create_database = PythonOperator(
        task_id='create_database_',
        python_callable=create_database,
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
    print_date >> create_database >> get_tickers >> get_financials >> query_stocks >> email_results
