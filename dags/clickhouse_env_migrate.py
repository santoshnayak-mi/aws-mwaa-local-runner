from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import pandas as pd
from clickhouse_driver import Client
import logging
import os
import pytz
from airflow.exceptions import AirflowSkipException,AirflowFailException

# Define the temporary directory for saving batch data
TEMP_DIR = "/tmp/airflow_batches"

# Function to select data in batches from ClickHouse
def select_clickhouse_data(batch_size=10000, **context):
    logger = logging.getLogger("airflow.task")
    os.makedirs(TEMP_DIR, exist_ok=True)  # Ensure the temporary directory exists
    all_files = []

    # ClickHouse connection
    client = Client(
        host='anx-us-clickhouse3.analytics.engageplatform.com',
        user='ro1-user',
        password='12G9cn2RY1CPwI7DkNbbMfGqebRrxWjzJRL0g7FwlZHpz3Lim0Crk1OCwASSG',
        database='anx_us_mercury',
        port=9000
    )
    
    # Query data with batching
    offset = 0
    batch_index = 0
    while True:
        query = f"""
        SELECT * FROM anx_us_mercury.event 
        WHERE account_id = '986' AND domain = 'event.campaignactivity' 
        AND date BETWEEN '2025-01-01' AND '2025-01-02'
        --and profile_alt_id = 'Davecastelow@yahoo.com'
        AND platform = 'msg:na'
        LIMIT {batch_size} OFFSET {offset}
        """
        results = client.execute(query)
        
        # If no results, exit the loop
        if not results:
            break
        
        # Convert to DataFrame
        columns = [column[0] for column in client.execute('DESCRIBE TABLE anx_us_mercury.event')]
        df = pd.DataFrame(results, columns=columns)
        
        # Save batch to temporary file
        batch_file = os.path.join(TEMP_DIR, f"batch_{batch_index}.json")
        df.to_json(batch_file, orient='records')
        all_files.append(batch_file)
        logger.info(f"Saved batch {batch_index} with {len(df)} rows to {batch_file}")
        
        # Increment batch details
        offset += batch_size
        batch_index += 1

    # Push all batch file paths as a single XCom key
    context['ti'].xcom_push(key="all_batch_files", value=all_files)
    logger.info(f"All batch file paths pushed to XCom: {all_files}")

    logger.info("All batches saved and pushed to XCom.")

# Function to insert data batches into ClickHouse
def insert_clickhouse_data(**context):
    logger = logging.getLogger("airflow.task")
    
    # ClickHouse connection
    client = Client(
        host='anx-stg1-clickhouse2.anx-staging.cdces.dev',
        user='rw1-user',
        password='9L01W9sd4CYivk7efwh3pbaK4WwrZmssvcfLZ9sqsKB2wXf2x5XMN7jRM9RQc',
        database='anx_stg1_mercury',
        port=9000
    )
    
    # Retrieve batch file paths from XCom
    ti = context['ti']
    all_files = ti.xcom_pull(task_ids='extract_clickhouse_data', key="all_batch_files")

    if not all_files:
        logger.error("No batch files found in XCom. Ensure extraction task pushed file paths.")
        raise AirflowSkipException("No batches to process.")

    # Retrieve the schema of the target table
    table_schema = client.execute("DESCRIBE TABLE mi_event")
    schema_dict = {column[0]: column[1] for column in table_schema}  # Map column names to data types
    logger.info(f"Retrieved ClickHouse schema: {schema_dict}")

    total_rows_inserted = 0

    for batch_index, batch_file in enumerate(all_files, start=1):
        if not os.path.exists(batch_file):
            logger.error(f"Batch file {batch_file} does not exist. Skipping.")
            continue

        # Load the batch data
        df = pd.read_json(batch_file)
        logger.info(f"Loaded batch {batch_index} with {len(df)} rows from {batch_file}")

        def clean_and_align_nested_structure(df, nested_structure_prefix, default_value_map):
            # Identify all nested structure columns
            nested_columns = [col for col in df.columns if col.startswith(f"{nested_structure_prefix}.")]

            def clean_and_align_row(row):
                # Replace None with an empty list for all nested columns
                arrays = {col: row[col] if isinstance(row[col], list) else [] for col in nested_columns}
                
                # Determine the maximum length of arrays in the row
                max_length = max(len(arr) for arr in arrays.values())

                # Pad each array to the maximum length using the default value map
                cleaned_arrays = {
                    col: (arr + [default_value_map[col[len(nested_structure_prefix) + 1:]]] * (max_length - len(arr)))
                    for col, arr in arrays.items()
                }
                return cleaned_arrays

            # Apply cleaning and alignment across all rows
            aligned_data = df.apply(lambda row: clean_and_align_row(row), axis=1)

            # Update the DataFrame with cleaned and aligned arrays
            for col in nested_columns:
                df[col] = aligned_data.apply(lambda x: x[col])
            return df
        
       # Check original column count
        original_column_count = df.shape[1]
        #logger.info(f"Original column count: {original_column_count}")

        # Align nested structure arrays
        #df = align_nested_structure_arrays(df, 'items')
        
        nested_columns = [col for col in df.columns if col.startswith("items.")]
        #logger.info(f"Nested structure columns: {nested_columns}")

        # Log the first few rows of the nested columns
        #for col in nested_columns:
        #  logger.info(f"First few rows of column '{col}': {df[col].head()}")
        
        #df['array_sizes'] = df.apply(lambda row: {col: len(row[col]) if isinstance(row[col], list) else 0 for col in nested_columns}, axis=1)
        #logger.info(f"Array sizes for each row: {df['array_sizes'].head()}")

        new_column_count = df.shape[1]
        #logger.info(f"New column count after alignment: {new_column_count}")

        if new_column_count != original_column_count:
            logger.error("Column count mismatch after alignment!")
            raise ValueError("Column count mismatch after alignment!")

        # Validate and transform the DataFrame based on the ClickHouse schema
        for column, datatype in schema_dict.items():
            if column in df.columns:
                if "Array(Map(String, Nullable(String)))" in datatype:
                    # Ensure valid arrays of maps
                    df[column] = df[column].apply(
                        lambda x: [x] if isinstance(x, dict) else ([] if pd.isna(x) else [dict(x)])
                    )
                    #logger.info(f"Transformed column '{column}': {df[column].head()}")

                elif "Map(String, Nullable(String))" in datatype or "Map(String, String)" in datatype:
                    # Convert plain strings to a map structure
                    df[column] = df[column].apply(
                        lambda x: {"key": str(x)} if pd.notna(x) else {}
                    )
                    #logger.info(f"Transformed column '{column}': {df[column].head()}")

                elif "Array(Map(String, Nullable(String)))" in datatype:
                    # Ensure valid arrays of maps
                    df[column] = df[column].apply(
                        lambda x: [x] if isinstance(x, dict) else ([] if pd.isna(x) else [dict(x)])
                    )
                    #logger.info(f"Transformed column '{column}': {df[column].head()}")

                elif "Array(String)" in datatype or "Array(LowCardinality(String))" in datatype or "Array(Nullable(String))" in datatype:
                    # Transform plain strings or nulls to an array of strings
                    # Ensure valid arrays
                    df[column] = df[column].apply(
                        lambda x: x if isinstance(x, list) else ([] if pd.isna(x) or x == '' else [str(x)])
                    )
                    df[column] = df[column].apply(
                        lambda arr: [str(item) if item is not None and pd.notna(item) else '' for item in arr]
                    )
                    #logger.info(f"Transformed column '{column}': {df[column].head()}")
                
                elif "Array(Nullable(Float64))" in datatype or "Array(Nullable(UInt32))" in datatype:
                    # Ensure valid arrays of floats
                    df[column] = df[column].apply(
                        lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [float(x)])
                    )

                elif "String" in datatype or "LowCardinality(Nullable(String))" in datatype or "Nullable(String)" in datatype:
                    # Convert column to string and handle NULL values
                    df[column] = df[column].apply(
                        lambda x: x if pd.notna(x) else ''
                    )
                    #df[column] = df[column].astype(str).where(df[column].notna(), None)
                    df[column] = df[column].apply(
                        lambda x: str(x) if pd.notna(x) else ''
                    )
                    #logger.info(f"Transformed column '{column}': {df[column].head()}")

                elif "UInt32" in datatype or "Nullable(Int32)" in datatype or "Nullable(UInt8)" in datatype:
                    # Convert to unsigned integer and handle invalid values
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    df[column] = df[column].apply(
                        lambda x: 0 if pd.isna(x) else int(x)
                    ).astype('uint32')

                elif "Date" in datatype or "Nullable(DateTime)" in datatype or "Nullable(Date)" in datatype:
                    # Parse dates and handle invalid values
                    def safe_date_conversion(item):
                        try:
                            if pd.isna(item):
                                return None
                            elif isinstance(item, str):
                                return datetime.fromisoformat(item)
                            elif isinstance(item, datetime):
                                return item
                            elif isinstance(item, int) or isinstance(item, float):  # Timestamps (if applicable)
                                return datetime.fromtimestamp(item)
                            else:
                                return None
                        except (ValueError, TypeError):
                            return None
                    
                    df[column] = df[column].apply(safe_date_conversion)
                    df[column] = df[column].apply(
                        lambda x: x.replace(tzinfo=pytz.UTC) if isinstance(x, datetime) and x.tzinfo is None else x
                    )
                    #logger.info(f"Transformed column '{column}': {df[column].head()}")

                elif "Nullable(Bool)" in datatype:
                    # Transform the profile_ignore column
                    df[column] = df[column].apply(
                        lambda x: None if pd.isna(x) else bool(x)
                    )

                elif "Float" in datatype or "Nullable(Float64)" in datatype:
                    # Convert to float and handle invalid values
                    #df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
                    df[column] = pd.to_numeric(df[column], errors='coerce').apply(lambda x: 0 if pd.isna(x) else float(x))

                # Log the validation step
                #logger.info(f"Column '{column}' validated and transformed to match {datatype}.")
    
        nested_structure_prefix = "items"
        #nested_columns = [col for col in df.columns if col.startswith(f"{nested_structure_prefix}.")]
        # Clean and align 'items' nested structure before insertion
        default_value_map = {
            'type': '',  # Default for 'items.type'
            'category': '',
            'subcategory': '',
            'name': '',
            'description': '',
            'sku': '',
            'upc': '',
            'baseprice': 0,
            'discount': 0,
            'discount_type': '',
            'netprice': 0,
            'quantity': 0,
            'item_attributes': {'key': None}  # Default for 'items.item_attributes'
        }
        #default_value_map['item_attributes'] = {'key': None}
        # 'items.subcategory', 'items.name', 'items.description', 'items.sku', 'items.upc', 'items.baseprice',
        #'items.discount', 'items.discount_type', 'items.netprice', 'items.quantity'
        # Dynamically build default_value_map
        #default_value_map = {
        #    col[len(nested_structure_prefix) + 1:]: '' if 'String' in schema_dict.get(col, '') else []
        #    for col in nested_columns
        #}
        
        #logger.info(f"Default value map: {default_value_map}")

        df = clean_and_align_nested_structure(df, nested_structure_prefix, default_value_map)

        # Convert DataFrame to a list of tuples for insertion
        data = [
            #tuple(row if pd.notna(row) else None for row in df_row) 
            tuple(row if not isinstance(row, list) or row is not None else [] for row in df_row)
            for df_row in df.values
        ]
        data = [
            tuple(
              [] if isinstance(row, list) and (row is None or all(item is None for item in row))
                 else (row if pd.notna(row) else None)
                 for row in df_row
            )
            for df_row in df.values
        ]

        #logger.info(f"Data '{data}'")
        # Insert data into ClickHouse
        insert_query = "INSERT INTO mi_event VALUES"
        try:
            client.execute(insert_query, data, types_check=True )  # Use types_check=True for strict validation
            total_rows_inserted += len(df)
            logger.info(f"Inserted batch {batch_index} with {len(df)} rows into ClickHouse. Total inserted: {total_rows_inserted}.")
        except Exception as e:
            logger.error(f"Error inserting batch {batch_index}: {e}")
            raise AirflowFailException(f"Insert operation failed for batch {batch_index}. Marking task as failed.")

    logger.info(f"Completed inserting all batches. Total rows inserted: {total_rows_inserted}.")


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'clickhouse_migration',
    default_args=default_args,
    description='DAG to select and insert data from ClickHouse using batching',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
) as dag:

    # Task to extract data from ClickHouse
    extract_data = PythonOperator(
        task_id='extract_clickhouse_data',
        python_callable=select_clickhouse_data,
        op_kwargs={'batch_size': 100000},
        provide_context=True,
    )

    # Task to load data into ClickHouse
    load_data = PythonOperator(
        task_id='load_clickhouse_data',
        python_callable=insert_clickhouse_data,
        provide_context=True,
    )

    # Define task dependencies
    extract_data >> load_data
