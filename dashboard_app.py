import streamlit as st
import glob
from pathlib import Path
import os
import pandas as pd


project_path = Path(os.getcwd()).joinpath('airflow_application').joinpath('dags').joinpath('data_weather_app').joinpath('processed_data')

all_files = glob.glob(f"/home/thiago/PycharmProjects/weather_application/aiflow_application/dags/data_weather_app/processed_data/*.parquet")
print(all_files)

all_df = []
for file in all_files:
    dfx = pd.read_parquet(file)
    all_df.append(dfx)

full_df = pd.concat(all_df)

print(full_df)
st.dataframe(full_df)

st.map(full_df)
st.area_chart(full_df[['temperatureAvg','temperatureMax','temperatureMin','time']],
              x_label='time',
              x='time')