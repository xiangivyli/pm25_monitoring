"""DAG that retrieves current pm25 information and loads it into DuckDB."""
# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from pendulum import datetime
import pandas as pd
#import random
# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c
from include.api_request import (
    #fetch_device_ids,
    get_last_7_days_pm25,
)

# -------- #
# Datasets #
# -------- #

start_dataset = Dataset("duckdb_pm25")

# --- #
# DAG #
# --- #

@dag(
    start_date=days_ago(1),
    # this DAG runs as soon as the "duckdb_pm25" Dataset has been produced to
    schedule=[start_dataset],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves device id from airbox project and saves it to a list.",
    tags=["step2"]
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
    
    @task
    def turn_df_into_table(duckdb_conn_id: str, pm25_table_name: str, pm25_df: pd.DataFrame):
        """
        Load it into DuckDB.
        Args:
            duckdb_conn_id (str): The connection ID for the DuckDB connection.
            pm25_table (str): The name of the table to be created in DuckDB.
            pm25_data (pd.DataFrame): The DataFrame input to be loaded into DuckDB.
        """
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        # Connect to DuckDB
        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()

        # Load DataFrame into DuckDB
        # Create empty table with correct schema
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {pm25_table_name} AS SELECT * FROM pm25_df LIMIT 0")  
        cursor.execute(
            "INSERT INTO {table} SELECT * FROM pm25_df", {'table': pm25_table_name})

        # Close the cursor
        cursor.close()


    
    # Choose one device ID
    device_ids = gv.device_IDs

    # Get data from this device
    pm25_data = get_pm25_data(device_ids)

    # Convert json to df and keep needed columns
    pm25_df = flatten_json_to_df(pm25_data)

    # Insert into database
    turn_df_into_table(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        pm25_table_name='pm25_data_table',
        pm25_df=pm25_df
    )

extract_pm25_to_db()