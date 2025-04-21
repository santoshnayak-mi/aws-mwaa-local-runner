
import pymssql
import logging
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'aws',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 20),
    'provide_context': True
}

dag = DAG(
    'mssql_conn_example', default_args=default_args, schedule_interval=None)


def get_db_connection():
    try:
        # Establish a connection to the MSSQL database
        connection = pymssql.connect(
           server='mi-clickhouse.ch4kyq7plgt4.us-east-1.rds.amazonaws.com',
            user='clickhousemi',
            password='db3OwD0DnoLJE8',
            database='xyz_dms_cust_986'
        )
        return connection
    except Exception as e:
        print(f"Failed to connect to database: {e}")


def create_table():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE dbo.member (name VARCHAR(20), owner VARCHAR(20));")
        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print(f"An error occurred during insertion: {e}")
    finally:
        if conn:
            conn.close()

def insert_into_table():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO dbo.member VALUES ('MITEST', 'Cheetah');")
        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print(f"An error occurred during insertion: {e}")
    finally:
        if conn:
            conn.close()

def select_from_table():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.member;")
        results = cursor.fetchall()
        for row in results:
            print(row)
    except Exception as e:
        print(f"An error occurred during selection: {e}")
    finally:
        if conn:
            conn.close()

def select_subs(**kwargs):
   try:
        conn = pymssql.connect(
            server='mi-clickhouse.ch4kyq7plgt4.us-east-1.rds.amazonaws.com',
            user='clickhousemi',
            password='db3OwD0DnoLJE8',
            database='xyz_dms_cust_986'
        )
        
        # Create a cursor from the connection
        cursor = conn.cursor()
        cursor.execute("SELECT top 10 * from xyz_dms_cust_986.dbo.subscribers")
        results = cursor.fetchall()
        for row in results:
            print(row)
   except:
      logging.error("Error when creating pymssql database connection: %s", sys.exc_info()[0])

select_query = PythonOperator(
    task_id='select_query',
    python_callable=select_subs,
    dag=dag,
)


# Create tasks for each step
create_table_task = PythonOperator(
    task_id="create_table_task",
    python_callable=create_table,
    dag=dag,
)

insert_task = PythonOperator(
    task_id="insert_task",
    python_callable=insert_into_table,
    dag=dag,
)

select_task = PythonOperator(
    task_id="select_task",
    python_callable=select_from_table,
    dag=dag,
)

# Set task dependencies
create_table_task >> insert_task >> select_task >> select_query