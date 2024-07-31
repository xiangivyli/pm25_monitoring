# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from pendulum import duration

# ----------------------- #
# Configuration variables #
# ----------------------- #

# API URL
api_url = "https://pm25.lass-net.org/API-1.0.0/project/airbox/latest/"

# device id
device_IDs = ["74DA38F7C254"]

# DuckDB config
DB_PATH = "include/pm25_ducks.db"
CONN_ID_DUCKDB = "my_local_duckdb_conn"
RAW_DUCKDB_PM = "pm25_data_table"
REPORTING_DUCKDB_PM = "combined_daily_pm25_stats"
DANGER_TIME_LIST = "danger_time_list"

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}
