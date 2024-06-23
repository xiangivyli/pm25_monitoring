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
                airbox_entries = feed.get('AirBox', [])
                for airbox in airbox_entries:
                    timestamp = airbox.get('time')
                    flattened_record = {
                        'device_id': device_id,
                        'source': source,
                        'timestamp': timestamp,
                        'date': airbox.get('date'),
                        'gps_lat': airbox.get('gps_lat'),
                        'gps_lon': airbox.get('gps_lon'),
                        's_d0': airbox.get('s_d0'),
                        's_d1': airbox.get('s_d1'),
                        's_d2': airbox.get('s_d2'),
                        's_h0': airbox.get('s_h0'),
                        's_t0': airbox.get('s_t0'),
                    }
                    flattened_data.append(flattened_record)       
        # Convert to DataFrame
        pm25_df = pd.DataFrame(flattened_data)
        return pm25_df

    @task
    def get_existing_timestamps(conn_str: str, pm25_table_name: str):
        """Retrieve existing timestamps from DuckDB to avoid duplicates."""
        conn = duckdb.connect(conn_str)
        try:
            # Check if the table exists
            table_exists_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{pm25_table_name}'"
            table_exists = conn.execute(table_exists_query).fetchone()[0] > 0
            
            if not table_exists:
                return set()

            # Retrieve existing timestamps
            query = f"SELECT DISTINCT timestamp FROM {pm25_table_name}"
            existing_timestamps = conn.execute(query).fetchall()
        except Exception as e:
            existing_timestamps = []
        finally:
            conn.close()
        return set(ts[0] for ts in existing_timestamps)

    @task
    def filter_new_data(pm25_df: pd.DataFrame, existing_timestamps: set):
        """Filter out records that already exist in DuckDB."""
        pm25_df["timestamp"] = pd.to_datetime(pm25_df["timestamp"])
        return pm25_df[~pm25_df['timestamp'].isin(existing_timestamps)]

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
            s_t0 DOUBLE,
            UNIQUE(device_id, timestamp)
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
    

    @task
    def check_for_duplicates(conn_str: str, pm25_table_name: str):
        """Check for duplicate timestamps in the DuckDB table."""
        conn = duckdb.connect(conn_str)
        query = f"""
        SELECT device_id, timestamp, COUNT(*) as count
        FROM {pm25_table_name}
        WHERE timestamp is NOT NULL
        GROUP BY device_id, timestamp
        HAVING COUNT(*) > 1
        """
        try:
            duplicates = conn.execute(query).fetchall()
        except duckdb.CatalogException:
            print(f"Table {pm25_table_name} does not exist. Exiting safely.")
            return
        finally:
            conn.close()

        if duplicates:
            raise ValueError(f"Duplicate timestamps found: {duplicates}")


    @task
    def check_timestamp_datatype(conn_str: str, pm25_table_name: str):
        """Check that the timestamp column has the correct datatype."""
        conn = duckdb.connect(conn_str)
        query = f"""
        PRAGMA table_info({pm25_table_name})
        """
        try:
            result = conn.execute(query).fetchall()
        except duckdb.CatalogException:
            print(f"Table {pm25_table_name} does not exist. Exiting safely.")
            return
        finally:
            conn.close()

        for column in result:
            col_name, col_type = column[1], column[2]
            if col_name == 'timestamp' and col_type != 'TIMESTAMP':
                raise ValueError(f"Timestamp column has incorrect datatype: {col_type}")
        
    # Choose one device ID
    device_ids = gv.device_IDs

    # Get data from this device
    pm25_data = get_pm25_data(device_ids)

    # Convert json to df and keep needed columns
    pm25_df = flatten_json_to_df(pm25_data)

    # Get existing timestamps from DuckDB
    existing_timestamps = get_existing_timestamps(conn_str=gv.DB_PATH, pm25_table_name=gv.RAW_DUCKDB_PM)

    # Filter out records that already exist in DuckDB
    new_pm25_df = filter_new_data(pm25_df, existing_timestamps)

    # Insert into database
    turn_df_into_table(
        conn_str=gv.DB_PATH,
        pm25_table_name=gv.RAW_DUCKDB_PM,
        pm25_df=new_pm25_df
    )

    # Check for duplicate data in the table
    check_for_duplicates(conn_str=gv.DB_PATH, pm25_table_name=gv.RAW_DUCKDB_PM)

    # Check that the timestamp column has the correct datatype
    check_timestamp_datatype(conn_str=gv.DB_PATH, pm25_table_name=gv.RAW_DUCKDB_PM)

    # This task uses the BashOperator to run a bash command creating an Airflow
    # pool called 'duckdb' which contains one worker slot. All tasks running
    # queries against DuckDB will be assigned to this pool, preventing parallel 
    # requests to DuckDB.
    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool",
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'"
    )

extract_pm25_to_db()