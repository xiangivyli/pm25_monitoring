# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
from pendulum import duration

# ----------------------- #
# Configuration variables #
# ----------------------- #

# Datasets
DS_START = Dataset("start")

# API URL
api_url = "https://pm25.lass-net.org/API-1.0.0/project/airbox/latest/"

# device id
device_IDs = ["74DA38F7C4B0"]

# DuckDB config
CONN_ID_DUCKDB = "duckdb_default"
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}
