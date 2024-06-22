"""Define Dataset and create a pool at the beginning"""

from airflow import Dataset
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv

# -------- #
# Datasets #
# -------- #

start_dataset = Dataset("start")

# --- #
# DAG #
# --- #
@dag(
    start_date=days_ago(1),
    # after being unpaused this DAG will run once
    schedule="@once",
    catchup=False,
    default_args=gv.default_args,
    description="Run this DAG to start the pipeline!",
    tags=["step1"],
)
def start_db_pool():

    # this task uses the BashOperator to run a bash command creating an Airflow
    # pool called 'duckdb' which contains one worker slot. All tasks running
    # queries against DuckDB will be assigned to this pool, preventing parallel
    # requests to DuckDB.
    create_duckdb_pool = BashOperator(
        task_id="bash_pool_set",
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'",
        outlets=[gv.DS_START],
    )


# when using the @dag decorator, the decorated function needs to be
# called after the function definition
start_dag = start_db_pool()