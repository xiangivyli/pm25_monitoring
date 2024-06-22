"""DAG that generates reporting table and loads it into DuckDB."""
# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from pendulum import datetime

from duckdb_provider.hooks.duckdb_hook import DuckDBHook


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
        conn_id: str, source_table: str, dest_table: str):
        """
        Query DuckDB to calculate daily statistics and store in a new table
        Args:
            conn_id (str): default DuckDB connection.
            source_table (str): the name of table to be queried
            dest_table (str): the name of table to be created"""
        
        # Connect to DuckDB
        my_duck_hook = DuckDBHook.get_hook(conn_id)
        conn = my_duck_hook.get_conn()

        # Query to calculate daily statistics
        combined_query = f"""
        SELECT 
           device_id,
           date,
           MAX(GREATEST(s_d0, s_d1, s_d2)) AS max_s_d,
           MIN(LEAST(s_d0, s_d1, s_d2)) AS min_s_d,
           AVG((s_d0 + s_d1 + s_d2) / 3) AS avg_s_d
        FROM {source_table}
        GROUP BY device_id, date;
        """

        # Execute the combined query and fetch the results into a DataFrame
        combined_daily_stats_df = conn.execute(combined_query).fetchdf()

        # Create a new table with the combined results
        conn.execute(f"CREATE TABLE IF NOT EXISTS {dest_table} AS SELECT * FROM combined_daily_stats_df")

        # Close the connection
        conn.close()

    # Run this task
    calculate_daily_stats(
        conn_id=gv.CONN_ID_DUCKDB,
        source_table=gv.RAW_DUCKDB_PM,
        dest_table=gv.REPORTING_DUCKDB_PM
    )
    
    @task
    def list_danger_time(
        conn_id: str, source_table: str, dest_table: str):
        """
        Query DuckDB to calculate average value for each time point,
        and list dangerous time which is above 30
        Args:
            conn_id (str): default DuckDB connection.
            source_table (str): the name of table to be queried
            dest_table (str): the name of table to be created"""
    
        # Connect to DuckDB
        my_duck_hook = DuckDBHook.get_hook(conn_id)
        conn = my_duck_hook.get_conn()

        # Query to find time when average s_d is above 30
        danger_query = f"""
        SELECT
            device_id,
            timestamp,
            (s_d0 + s_d1 + s_d2) / 3 AS avg_s_d
        FROM {source_table}
        WHERE (s_d0 + s_d1 + s_d2) / 3 > 30;
        """

        # Execute the danger query and fetch the results into a DataFrame
        danger_times_df = conn.execute(danger_query).fetchdf()

        # Save the danger times to a new table
        conn.execute(f"CREATE TABLE IF NOT EXISTS danger_times AS SELECT * FROM danger_times_df")

        # Close the connection
        conn.close()

    # Run this task
    list_danger_time(
        conn_id=gv.CONN_ID_DUCKDB,
        source_table=gv.RAW_DUCKDB_PM,
        dest_table=gv.DANGER_TIME_LIST
    )

reporting_table()