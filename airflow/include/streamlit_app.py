# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import streamlit as st 
import duckdb
import pandas as pd 
from datetime import date, datetime
import global_variables.airflow_conf_variables as gv

# --------- #
# VARIABLES #
# --------- #

duck_db_instance_path = (
    "/app/include/pm25_ducks.db"
)
table_name = gv.REPORTING_DUCKDB_PM
list_name = gv.DANGER_TIME_LIST
# -------------- #
# DuckDB Queries #
# -------------- #

conn = duckdb.connect(database=duck_db_instance_path, read_only=True)
daily_data = conn.execute(f"SELECT * FROM {gv.REPORTING_DUCKDB_PM}").df()
danger_time_data = conn.execute(f"SELECT * FROM {gv.DANGER_TIME_LIST}").df()
conn.close()

# ------------- #
# STREAMLIT APP #
# ------------- #

st.title(f"PM2.5 Monitoring for {gv.device_IDs}")

st.subheader("SiteName:新北市清水國小")

# Display daily time data

if not daily_data.empty:
    st.dataframe(daily_data)
else:
    st.write("No data available.")

# Display danger time data
st.subheader("Danger Time Data (PM2.5 > 22)")

if not danger_time_data.empty:
    st.dataframe(danger_time_data, height=400)
else:
    st.write("No danger time data available.")

# Custom CSS for grey background
st.markdown(
    """
    <style>
    .main {
        background-color: #f0f0f0;
        color: #000000;
    }
    .stDataFrame {
        background-color: #ffffff;
        color: #000000;
        border-radius: 10px;
        padding: 10px;
        border: 1px solid #ddd;
    }
    .stTitle, .stSubheader {
        color: #333333;
    }
    .stTable {
        color: #000000;
        background-color: #ffffff;
        border-radius: 10px;
        padding: 10px;
        border: 1px solid #ddd;
    }
    .css-18e3th9 {
        padding: 2rem;
    }
    </style>
    """,
    unsafe_allow_html=True
)