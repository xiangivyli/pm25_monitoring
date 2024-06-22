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
def extract_device_id_airbox():
    """
    this task run too slow as there are too many missing values, I chose a valid id directly
    # use the /project/{project}/latest endpoint to extract active device ids
    @task
    def get_one_valid_device_id():
        """use the 'fetch_device_ids' function from the local 'metereology_utils'
        module to retrieve only 1 active device id from airbox project"""

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
    
    # use the /device/<device_id>/history/ endpoint to extract pm25 information
    @task
    def get_pm25_data(device_ids):
        """use the 'get_last_7_days_pm25' function from the local 'meterology_utils'
        module to retrieve the last 7 days data which updated 5 mins"""
        all_device_data = []
        for device_id in device_ids:
            device_data = get_last_7_days_pm25(device_id)
            if device_data:
                all_device_data.append(device_data)
        return all_device_data

    # task to convert JSON to DataFrame
    @task
    def flatten_json_to_df(json_data):
            flattened_data = []
        
            for record in json_data:
                device_id = record.get('device_id')
                source = record.get('source')
                num_of_records = record.get('num_of_records')
                
                for feed in record.get('feeds', []):
                    for airbox in feed.get('AirBox', []):
                        for timestamp, data in airbox.items():
                            flattened_record = {
                                'device_id': device_id,
                                'source': source,
                                'num_of_records': num_of_records,
                                'timestamp': timestamp
                            }
                            flattened_record.update(data)
                            flattened_data.append(flattened_record)
            # convert to dataframe
            df = pd.DataFrame(flattened_data)
            return df
    
    
    
    # insert into DuckDB
    @task
    def turn_df_into_table(
        duckdb_conn_id: str, pm25_table_name: str, pm25_data: list
        ):
        """
        Convert the JSON input with info about the current weather into a pandas
        DataFrame and load it into DuckDB.
        Args:
            duckdb_conn_id (str): The connection ID for the DuckDB connection.
            pm25_table_name (str): The name of the table to be created in DuckDB.
            pm25_data (list): The JSON input to be loaded into DuckDB.
        """
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()
        cursor.sql(
            f"CREATE TABLE IF NOT EXISTS {pm25_table_name} AS SELECT * FROM pm25_df"
        )
        cursor.sql(
            f"INSERT INTO {pm25_table_name} SELECT * FROM pm25_df"
        )
        cursor.close()


    api_url = "https://pm25.lass-net.org/API-1.0.0/project/airbox/latest/"
    # Choose one device ID
    device_ids = ["74DA38F7C4B0"]
    # Get data from this device
    pm25_data = get_pm25_data(device_ids)
    turn_json_into_table(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        pm25_table_name='pm25_data_table',
        pm25_data=pm25_data
    )

extract_device_id_airbox()