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
    "pm25_ducks.db"
)
table_name = gv.REPORTING_DUCKDB_PM
list_name = gv.DANGER_TIME_LIST
# -------------- #
# DuckDB Queries #
# -------------- #

def daily_data_read(db=duck_db_instance_path, table_name=table_name):
    try:
        conn = duckdb.connect(db)

        # Query to retrieve data from daily table
        query = f"""SELECT *
        FROM {table_name}"""

        # Execute the query
        cursor = conn.execute(query)
        daily_data_df = cursor.fetchdf()

        conn.close()

        return daily_data_df

    except Exception as e:
        st.error(f"Error reading data: {e}")
        return pd.DataFrame()

def danger_time_read(db=duck_db_instance_path, table_name=list_name):
    try:
        conn = duckdb.connect(db)

        # Query to retrieve data from daily table
        query = f"""SELECT *
        FROM {table_name}"""

        # Execute the query
        cursor = conn.execute(query)
        danger_data_df = cursor.fetchdf()

        conn.close()

        return danger_data_df

    except Exception as e:
        st.error(f"Error reading data: {e}")
        return pd.DataFrame()

# ------------- #
# STREAMLIT APP #
# ------------- #

st.title(f"PM2.5 Monitoring for {gv.device_IDs}")

st.subheader("SiteName:新北市清水國小")

# Display daily time data
daily_data = daily_data_read()
if not daily_data.empty:
    st.dataframe(daily_data)
else:
    st.write("No data available.")

# Display danger time data
st.subheader("Danger Time Data")
danger_time_data = danger_time_read()
if not danger_time_data.empty:
    st.dataframe(danger_time_data)
else:
    st.write("No danger time data available.")
