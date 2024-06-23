"""DAG that generates reporting table and loads it into DuckDB."""
# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from pendulum import datetime

import pandas as pd
import duckdb


# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv

# -------- #
# Datasets #
# -------- #

Upstream_Dataset = Dataset("duckdb://include/pm25_raw")

# --- #
# DAG #
# --- #

@dag(
    start_date=days_ago(1),
    schedule=[Upstream_Dataset],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that aggregates data from pm25_data_table",
    tags=["step2", "duckdb"]
)
def reporting_table():
    @task(
           pool="duckdb", outlets=[Dataset("duckdb://include/pm25_report")]
    )
    def calculate_daily_stats(
        conn_str: str, source_table: str, dest_table: str):
        """
        Query DuckDB to calculate daily statistics and store in a new table
        Args:
            conn_str (str): default DuckDB connection.
            source_table (str): the name of table to be queried
            dest_table (str): the name of table to be created"""
        conn = duckdb.connect(conn_str)
        cursor = conn.cursor()

        # Query to calculate daily statistics
        combined_query = f"""
        SELECT 
           device_id,
           date,
           MAX(GREATEST(s_d0, s_d1, s_d2)) AS max_pm25,
           MIN(LEAST(s_d0, s_d1, s_d2)) AS min_pm25,
           AVG((s_d0 + s_d1 + s_d2) / 3) AS avg_pm25
        FROM {source_table}
        GROUP BY device_id, date;
        """

        # Execute the combined query and fetch the results into a DataFrame
        combined_daily_stats_df = conn.execute(combined_query).fetchdf()
        
        # Create empty table with correct schema
        create_daily_table_query = f"""
        CREATE OR REPLACE TABLE {dest_table} (
            device_id TEXT,
            date DATE,
            max_pm25 DOUBLE,
            min_pm25 DOUBLE,
            avg_pm25 DOUBLE,
            UNIQUE(device_id, date)
        );
        """
        cursor.execute(create_daily_table_query)

        # Insert data into daily table
        cursor.register("combined_daily_stats_df_view", combined_daily_stats_df)
        insert_query = f"INSERT INTO {dest_table} SELECT * FROM combined_daily_stats_df_view"
        cursor.execute(insert_query)

        # Close the cursor
        conn.commit()
        cursor.close()
        conn.close()

    # Run this task
    calculate_daily_stats(
        conn_str=gv.DB_PATH,
        source_table=gv.RAW_DUCKDB_PM,
        dest_table=gv.REPORTING_DUCKDB_PM
    )
    
    @task()
    def list_danger_time(
        conn_str: str, source_table: str, dest_table: str):
        """
        Query DuckDB to calculate average value for each time point,
        and list dangerous time which is above 30
        Args:
            conn_str (str): default DuckDB connection.
            source_table (str): the name of table to be queried
            dest_table (str): the name of table to be created"""
    
        # Connect to DuckDB
        conn = duckdb.connect(conn_str)
        cursor = conn.cursor()

        # Query to find time when average s_d is above 20
        danger_query = f"""
        SELECT
            device_id,
            timestamp,
            (s_d0 + s_d1 + s_d2) / 3 AS avg_pm25
        FROM {source_table}
        WHERE (s_d0 + s_d1 + s_d2) / 3 >= 22;
        """

        # Execute the danger query and fetch the results into a DataFrame
        danger_times_df = conn.execute(danger_query).fetchdf()

        danger_times_df["timestamp"] = pd.to_datetime(danger_times_df["timestamp"], utc=False)

        # Create empty table with correct schema
        create_danger_time_query = f"""
        CREATE OR REPLACE TABLE {dest_table} (
            device_id TEXT,
            timestamp TIMESTAMP,
            avg_pm25 DOUBLE,
        );
        """
        conn.execute(create_danger_time_query)

        # Insert data into danger table
        cursor.register("danger_time_df_view", danger_times_df)
        insert_danger_query = f"INSERT INTO {dest_table} SELECT * FROM danger_time_df_view"
        cursor.execute(insert_danger_query)
        
        # Close the cursor
        conn.commit()
        cursor.close()
        conn.close()

    # Run this task
    list_danger_time(
        conn_str=gv.DB_PATH,
        source_table=gv.RAW_DUCKDB_PM,
        dest_table=gv.DANGER_TIME_LIST
    )

reporting_table()