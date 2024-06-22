"""DAG that retrieves current pm25 information and loads it into DuckDB."""
# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import duckdb
#import random
from airflow.operators.bash import BashOperator

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.api_request import (
    #fetch_device_ids,
    get_last_7_days_pm25,
)


# --- #
# DAG #
# --- #

@dag(
    start_date=datetime(2024, 6, 20),
    # this DAG runs once after unpaused, then run once at midnight
    schedule="@daily",
    catchup=False,
    default_args=gv.default_args,
    description="DAG that extracts data from api and store in DuckDB.",
    tags=["step1", "api", "device_id", "duckdb"]
)
def extract_pm25_to_db():


    """
    this task run too slow as there are too many missing values, I chose a valid id directly
    # use the /project/{project}/latest endpoint to extract active device ids
    @task
    def get_one_valid_device_id():
        # use the 'fetch_device_ids' function from the local 'api_request'
        # module to retrieve only 1 active device id from airbox project

        device_ids = fetch_device_ids(api_url)

        random.shuffle(device_ids)
    
        for device_id in device_ids:
            device_data = get_last_7_days_pm25(device_id)
            if device_data and 'feeds' in device_data:
                for feed in device_data['feeds']:
                    airbox_entries = feed.get('AirBox', [])
                    if any(entry.get('time') is not None for entry in airbox_entries):
                        return device_id
    
        raise ValueError("No devices with valid timestamps found.")
    """

    @task
    def get_pm25_data(device_ids):
        """Retrieve the last 7 days of PM2.5 data for each device ID."""
        all_device_data = []
        for device_id in device_ids:
            device_data = get_last_7_days_pm25(device_id)
            if device_data:
                all_device_data.append(device_data)
        return all_device_data

    @task
    def flatten_json_to_df(all_device_data):
        """Flatten JSON data, keep needed columns and convert it to a pandas DataFrame."""
        flattened_data = []
   
        for record in all_device_data:
            device_id = record.get('device_id')
            source = record.get('source')
            
            for feed in record.get('feeds', []):
                for airbox in feed.get('AirBox', []):
                    for timestamp, data in airbox.items():
                        flattened_record = {
                            'device_id': device_id,
                            'source': source,
                            'timestamp': timestamp,
                            'date': data.get('date'),
                            'gps_lat': data.get('gps_lat'),
                            'gps_lon': data.get('gps_lon'),
                            's_d0': data.get('s_d0'),
                            's_d1': data.get('s_d1'),
                            's_d2': data.get('s_d2'),
                            's_h0': data.get('s_h0'),
                            's_t0': data.get('s_t0'),
                        }
                        flattened_data.append(flattened_record)
        
        # Convert to DataFrame
        pm25_df = pd.DataFrame(flattened_data)
        return pm25_df
    
    @task(
        pool="duckdb", outlets=[Dataset("duckdb://include/pm25_raw")]
    )
    def turn_df_into_table(
        conn_str: str, pm25_table_name: str, pm25_df: pd.DataFrame):
        """
        Load it into DuckDB.
        Args:
           conn_str (str): path to the DuckDB database file.
           pm25_table_name (str): the name of the table to be created in DuckDB.
           pm25_df (pd.DataFrame): the DataFrame to be loaded into DuckDB"""

        # Connect to DuckDB
        conn = duckdb.connect(conn_str)
        cursor = conn.cursor()

        # Create empty table with correct schema
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {pm25_table_name} (
            device_id TEXT,
            source TEXT,
            timestamp TIMESTAMP,
            date DATE,
            gps_lat DOUBLE,
            gps_lon DOUBLE,
            s_d0 DOUBLE,
            s_d1 DOUBLE,
            s_d2 DOUBLE,
            s_h0 DOUBLE,
            s_t0 DOUBLE
        );
        """
        cursor.execute(create_table_query)

        # Insert data into DuckDB table
        cursor.register("pm25_df_view", pm25_df)
        insert_query = f"INSERT INTO {pm25_table_name} SELECT * FROM pm25_df_view"
        cursor.execute(insert_query)
    
        # Commit the transaction and close the cursor
        conn.commit()
        cursor.close()
        conn.close()
    
    # Choose one device ID
    device_ids = gv.device_IDs

    # Get data from this device
    pm25_data = get_pm25_data(device_ids)

    # Convert json to df and keep needed columns
    pm25_df = flatten_json_to_df(pm25_data)

    # Insert into database
    turn_df_into_table(
        conn_str=gv.DB_PATH,
        pm25_table_name=gv.RAW_DUCKDB_PM,
        pm25_df=pm25_df
    )

    # This task uses the BashOperator to run a bash command creating an Airflow
    # pool called 'duckdb' which contains one worker slot. All tasks running
    # queries against DuckDB will be assigned to this pool, preventing parallel
    # requests to DuckDB.
    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool",
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'"
    )

extract_pm25_to_db()