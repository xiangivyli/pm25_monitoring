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
table_name = gv.DANGER_TIME_LIST
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
        daily_data = conn.execute(query).fetchall()

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]

        conn.close()

        # Create a DataFrame from the fetched data
        df = pd.DataFrame(
            daily_data, columns=column_names
        )
        return df

    except Exception as e:
        st.error(f"Error reading data: {e}")
        return pd.DataFrame()


# ------------- #
# STREAMLIT APP #
# ------------- #

st.title(f"Daily PM2.5 Data for {gv.device_IDs}")

st.subheader("Data List")

daily_data = daily_data_read()

if not daily_data.empty:
    st.dataframe(daily_data)
else:
    st.write("No data available.")




