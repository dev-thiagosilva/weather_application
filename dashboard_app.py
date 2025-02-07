import streamlit as st
import glob
from pathlib import Path
import os
import pandas as pd

project_path = Path(os.getcwd()).joinpath('airflow_application').joinpath('dags').joinpath('data_weather_app').joinpath(
    'processed_data')

all_files = glob.glob(f"/home/thiago/PycharmProjects/weather_application/aiflow_application/dags/data_weather_app"
                      f"/processed_data/*.parquet")
if len(all_files) == 0:
    all_files = glob.glob(f"{Path(__file__).resolve().parent}/aiflow_application/dags/data_weather_app/processed_data"
                          f"/*.parquet")

all_df = []
for file in all_files:
    dfx = pd.read_parquet(file)
    all_df.append(dfx)

full_df = pd.concat(all_df)

options = st.multiselect(
    "Select columns",
    full_df.columns.tolist(),
    default=['temperatureAvg', 'temperatureMax', 'temperatureMin']
)

regions = st.multiselect(
    "Select columns",
    full_df['address'].unique().tolist(),
    default=full_df['address'].unique().tolist()[0]
)

full_df = full_df[full_df['address'].isin(regions)]

st.map(full_df)
st.area_chart(full_df[options + ['time']],
              x_label='time',
              x='time')
st.dataframe(full_df[options])

