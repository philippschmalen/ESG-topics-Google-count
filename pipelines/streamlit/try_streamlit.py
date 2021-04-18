import pandas as pd 
import streamlit as st


@st.cache
def get_UN_data():
    AWS_BUCKET_URL = "https://streamlit-demo-data.s3-us-west-2.amazonaws.com"
    df = pd.read_csv(AWS_BUCKET_URL + "/agri.csv.gz")
    return df.set_index("Region")

df = get_UN_data()
# print(df.head().to_markdown())

df
