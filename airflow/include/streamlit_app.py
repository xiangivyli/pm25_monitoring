import streamlit as st 
import pandas as pd 

st.write("""
# My first app
Hello *world!*
""")

df = pd.read_csv("my_date.csv")
st.line_chart(df)