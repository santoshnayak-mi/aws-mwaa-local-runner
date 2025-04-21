from airflow import DAG
from clickhouse_driver import Client
from datetime import datetime
import subprocess
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
import pymssql
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
                    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2025, 3, 10),  # Adjust as necessary
}


def get_sql_db_connection():
    #try:
    #     # Establish a connection using SQLAlchemy instead of pymssql
    #     engine = create_engine("mssql+pymssql://clickhousemi:db3OwD0DnoLJE8@mi-clickhouse.ch4kyq7plgt4.us-east-1.rds.amazonaws.com/xyz_dms_cust_986?charset=utf8")
    #     return engine
    # except Exception as e:
    #     logging.error(f"Failed to connect to database: {e}")
    #     return None
    try:
        # Establish a connection to the MSSQL database
        connection = pymssql.connect(
           server='mi-clickhouse.ch4kyq7plgt4.us-east-1.rds.amazonaws.com',
            user='clickhousemi',
            password='db3OwD0DnoLJE8',
            database='xyz_dms_cust_986',
            timeout=60
        )
        return connection
    except Exception as e:
        print(f"Failed to connect to database: {e}")

def select_from_sql_table(batch_size,temp_dir='/tmp/airflow_batches', **context):
    conn = None
    total_rows_selected = 0

    try:
        os.makedirs(temp_dir, exist_ok=True)
        logging.info(f"Temporary directory created or already exists: {temp_dir}")
    except Exception as e:
        logging.error(f"Failed to create temporary directory {temp_dir}: {e}")
        return
    
    try:
        conn = get_sql_db_connection()
        if conn is None:
            logging.error("Database connection could not be established.")
            return
        
        offset = 0
        batch_index = 0
        all_files = []

        while True:
            query = f"""
            --SELECT *
            --FROM (
                SELECT [pk_g4_subscribers_id],[p_email],[origination],[testgroup],[ms_orig_src]
              --  , ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn 
                FROM [xyz_dms_cust_986].[MIP].[g4_subs]
                WHERE pk_g4_subscribers_id > {offset} AND pk_g4_subscribers_id <= {offset + batch_size}
            --) AS sub
            -- WHERE sub.rn > {offset} AND sub.rn <= {offset + batch_size}
            """
            #df_batch = pd.read_sql(query, conn)
            df_batch = pd.read_sql(query, conn)
            #df_batch = df_batch.astype(str).apply(lambda x: x.str.encode('utf-8', 'ignore').str.decode('utf-8'))

            rows_in_batch = len(df_batch)

            if df_batch.empty:
                break
            
            batch_file = os.path.join(temp_dir, f"batch_{batch_index}.json")
            #df_batch.to_json(batch_file, orient='records', force_ascii=False, encoding='utf-8', errors='replace')
            df_batch.to_json(batch_file, orient='records')
            logging.info(f"Saved batch {batch_index} with {rows_in_batch} rows to {batch_file}.")

            all_files.append(batch_file)  # Add file path to list
            offset += batch_size
            batch_index += 1

         # Push all file paths as a single XCom entry
        context['ti'].xcom_push(key="all_batch_files", value=all_files)
        logging.info(f"All batch files pushed to XCom: {all_files}")

        logging.info("All batches read successfully!!")        

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


def insert_clickhouse_data(batch_size,temp_dir='/tmp/airflow_batches', **context):
    logger = logging.getLogger("airflow.task")
    # Using the ClickHouse connection
    client = Client(
        host='anx-stg1-clickhouse1.anx-staging.cdces.dev',
        user='rw1-user',
        password='9L01W9sd4CYivk7efwh3pbaK4WwrZmssvcfLZ9sqsKB2wXf2x5XMN7jRM9RQc',
        database='anx_stg1_mercury',
        port=9000
    )
    total_rows_inserted = 0

    ti = context['ti']
    all_files = ti.xcom_pull(task_ids='extract_data_from_sql', key="all_batch_files")

    if not all_files:
        logging.error("No batch files found in XCom. Ensure extraction task pushed file paths.")
        raise AirflowSkipException("No batches to process.")

    for i, batch_file in enumerate(all_files, start=1):
        if not os.path.exists(batch_file):
            logging.error(f"Batch file {batch_file} does not exist. Skipping.")
            continue

        df_batch = pd.read_json(batch_file)
        logging.info(f"Inserting batch {i}, Rows: {len(df_batch)}")
        rows_in_batch = len(df_batch)

        df_batch = df_batch.fillna('')
        
        data = [tuple(row) for row in df_batch.values]
        for row in data:
            if any(value is None for value in row):  # Additional check for None
                logging.warning(f"Row contains None: {row}")

        # Create ClickHouse table schema dynamically for the first batch        
        if i == 1:
            columns = ", ".join([f"{col} {get_clickhouse_type(dtype)}" for col, dtype in zip(df_batch.columns, df_batch.dtypes)])
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS mi_subscribers_all (
                {columns}
            ) ENGINE = MergeTree() ORDER BY {df_batch.columns[0]};
            """
            logging.info("Creating ClickHouse table with schema:")
            logging.info(create_table_query)
            client.execute(create_table_query)

        data = [tuple(row) for row in df_batch.values]
        insert_query = "INSERT INTO mi_subscribers_all VALUES"

        try:
            client.execute(insert_query, data)
            total_rows_inserted += rows_in_batch
            logging.info(f"Inserted batch {i} with {rows_in_batch} rows. Total rows inserted: {total_rows_inserted}.")
        except Exception as e:
            logging.error(f"Error inserting batch {i}: {e}")
            continue

    logging.info("All batches inserted successfully!")

def get_clickhouse_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "Int32"
        elif pd.api.types.is_float_dtype(dtype):
            return "Float32"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DateTime"
        else:
            return "String"

with DAG('sql_to_clickhouse_etl',
         default_args=default_args,
         description='A DAG to extract data from MS SQL Server and push to ClickHouse in batches',
         schedule_interval=None,
         catchup=False) as dag:

    extract_data = PythonOperator(
        task_id='extract_data_from_sql',
        python_callable=select_from_sql_table,
        op_kwargs={'batch_size': 1000000},
        provide_context=True,
    )

    load_data = PythonOperator(
        task_id='load_data_to_clickhouse',
        python_callable=insert_clickhouse_data,
        op_kwargs={'batch_size': 1000000},
        provide_context=True,
    )

    extract_data >> load_data