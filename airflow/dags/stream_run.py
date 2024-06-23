"""DAG that run streamlit"""
# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv

# -------- #
# Datasets #
# -------- #

Vis_Dataset = Dataset("duckdb://include/pm25_report")

# --- #
# DAG #
# --- #

@dag(
    start_date=days_ago(1),
    schedule=[Vis_Dataset],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that run streamlit",
    tags=["step3", "streamlit"],
)
def start_streamlit_app():
    """Bash Run streamlit run script.py"""
    run_streamlit = BashOperator(
        task_id="run_streamlit_app",
        bash_command="streamlit run include/streamlit_app.py",
    )

    run_streamlit

start_streamlit_app()