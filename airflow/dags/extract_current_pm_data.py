"""DAG that retrieves current pm25 information and loads it into DuckDB."""
# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv