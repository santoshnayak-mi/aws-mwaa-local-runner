from airflow import DAG
from clickhouse_driver import Client
from datetime import datetime
import subprocess
from airflow import DAG
#from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime
import pymssql
import logging
import pandas as pd
from sqlalchemy import create_engine
#import pyarrow as pa

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2025, 3, 2),  # Adjust as necessary
}

dag = DAG(
    'mssql_to_clickhouse', 
    default_args=default_args, 
    description='A simple DAG to extract data from MS SQL Server and push to ClickHouse',
    schedule_interval=None,
    catchup=False)


def get_sql_db_connection():
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

def select_from_sql_table():
    conn = None
    try:
        conn = get_sql_db_connection()
        #cursor = conn.cursor()
        query = "SELECT top 100000 * FROM dbo.subscribers;"
        #cursor.execute("SELECT top 10 * FROM dbo.subscribers;")
        #results = cursor.fetchall()

        #df = pd.read_sql(query, conn)
        #column_definitions = ", ".join([f"{col} {pd.io.sql.get_schema(df, 'dummy').split(f'{col} ')[1].split()[0]}" for col in df.columns])
        #print(column_definitions)

        # Create the SQLAlchemy engine
        #engine = create_engine(conn)
    
        # Define the query to extract data
        #query = "SELECT * FROM your_table;"
    
        # Use pandas to execute the query and get the DataFrame
        df = pd.read_sql(query, conn)
        #print(df)
        return df
        #for row in results:
        #    print(row)
    except Exception as e:
        print(f"An error occurred during selection: {e}")
    finally:
        if conn:
            conn.close()


def insert_clickhouse_data(df):
    logger = logging.getLogger("airflow.task")
    # Using the ClickHouse connection
    client = Client(
        host='anx-stg1-clickhouse2.anx-staging.cdces.dev',
        user='rw1-user',
        password='9L01W9sd4CYivk7efwh3pbaK4WwrZmssvcfLZ9sqsKB2wXf2x5XMN7jRM9RQc',
        database='anx_stg1_mercury',
        port=9000
    )
    #engine = create_engine(client)
    # Example query
    results = client.execute('SELECT * FROM anx_stg1_mercury.campaign LIMIT 10')
    for row in results:
        print(row)
    
    columns = ", ".join([f"{col} {get_clickhouse_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS mi_subscribers (
        {columns}
    ) ENGINE = MergeTree() ORDER BY {df.columns[0]};
    """
    print(columns)
    print(create_table_query)
    # Execute the CREATE TABLE query
    client.execute(create_table_query)
    
    # Insert the DataFrame into ClickHouse
    insert_query = f"INSERT INTO mi_subscribers VALUES"
    data = [tuple(row) for row in df.values]
    client.execute(insert_query, data)
    
    print("Data loaded successfully!")

def get_clickhouse_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "Int32"
        elif pd.api.types.is_float_dtype(dtype):
            return "Float32"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DateTime"
        else:
            return "String"

def extract_transform_load(**kwargs):
    # Extract data from MS SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_connection')
    sql_query = "SELECT * FROM xyz_dms_cust_986.dbo.subscribers"
    records = mssql_hook.get_pandas_df(sql_query)

    # Transform data (if needed)
    # For simplicity, this example assumes no transformation is necessary.

    # Load data to ClickHouse
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_connection')
    clickhouse_hook.run("INSERT INTO your_clickhouse_table VALUES", records.to_records(index=False))


def extract_and_load():
    df = select_from_sql_table()
    insert_clickhouse_data(df)

etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_and_load,
        provide_context=True,
        dag=dag,
)

#extract = PythonOperator(
#    task_id='extract_query',
#    python_callable=select_from_sql_table,
#    dag=dag,
#)

#insert_task = PythonOperator(
#   task_id="load_task",
#   python_callable=insert_clickhouse_data,
#   dag=dag,
#)

extract_transform_load