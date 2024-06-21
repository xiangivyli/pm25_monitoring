# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset

# ----------------------- #
# Configuration variables #
# ----------------------- #

# Datasets
DS_START = Dataset("start")

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}
