from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_fetching import fetch_stock_data  # Import your fetch script

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='A simple financial data ETL pipeline',
    schedule=timedelta(days=1),
)

def fetch_and_store():
    symbol = 'AAPL'
    stock_data_df = fetch_stock_data.fetch_stock_data(symbol)
    fetch_stock_data.store_data_to_postgresql(stock_data_df)

fetch_and_store_task = PythonOperator(
    task_id='fetch_and_store_data',
    python_callable=fetch_and_store,
    dag=dag,
)

fetch_and_store_task
