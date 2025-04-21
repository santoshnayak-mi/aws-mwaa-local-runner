from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime
import logging


def query_clickhouse_data(**kwargs):
    logger = logging.getLogger("airflow.task")
    logger.info("This is an info log")
    logger.debug("This is a debug log")
    logger.error("This is an error log")
    # Using the ClickHouse connection
    client = Client(
        host='anx-stg1-clickhouse2.anx-staging.cdces.dev',
        user='rw1-user',
        password='9L01W9sd4CYivk7efwh3pbaK4WwrZmssvcfLZ9sqsKB2wXf2x5XMN7jRM9RQc',
        database='anx_stg1_mercury',
        port=9000
    )


    logging.info("This is a log message Clickhouse test MI*****************")
    # Example query
    results = client.execute('SELECT * FROM anx_stg1_mercury.campaign LIMIT 10')
    for row in results:
        print(row)


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG
with DAG(
    'clickhouse_query_example',
    default_args=default_args,
    description='Query data from ClickHouse using Airflow',
    schedule=None,
    catchup=False,
) as dag:
    
    query_data_task = PythonOperator(
        task_id='query_clickhouse_data',
        python_callable=query_clickhouse_data
    )

    query_data_task
