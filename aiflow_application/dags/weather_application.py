import traceback

import requests
import pandas as pd
import json
import datetime
from pathlib import Path
import os
import hashlib
import glob

# Creating path to local files
project_path = Path(__file__).resolve().parent.joinpath('data_weather_app')
project_path.mkdir(parents=True, exist_ok=True)

# Folder to raw data
raw_data_path = project_path.joinpath('raw_data')
raw_data_path.mkdir(parents=True, exist_ok=True)

# Folder related to staging
processed_data_path = project_path.joinpath('processed_data')
processed_data_path.mkdir(parents=True, exist_ok=True)

# Folder related to partitioning
partitioned_data_path = project_path.joinpath('partitioned_data')
partitioned_data_path.mkdir(parents=True, exist_ok=True)

def _search_latlon(user_input:str):
    dis_matrix_api = '52E2Gs71uO7V5aoZkYVsO0Dc2iMiBbnSh2AphRq8ksGQohOGMOP0OISg0BK8uEWD'
    distance_matrix = f"https://api.distancematrix.ai/maps/api/geocode/json?address={user_input}&key={dis_matrix_api}"

    response = requests.get(distance_matrix)
    response_json = json.loads(response.text)
    response_latlon = response_json['result'][0]["geometry"]["location"]

    return response_latlon, response_json

def _search_weather_condition(lat_lon: dict):
    latitude = lat_lon['lat']
    longitude = lat_lon['lng']
    print(latitude, longitude)

    tomorrow_api = 'O6laJOEXh1FVGBBUnaeErwhQjwyMfnem'
    tomorrow_io = f"https://api.tomorrow.io/v4/weather/forecast?location={latitude},{longitude}&apikey={tomorrow_api}"
    response = requests.get(tomorrow_io)
    response = json.loads(response.text)
    return response

def _generate_raw_json(user_input, data_matrix_json: dict, tomorrow_io_info: dict):

    dict_raw = {
        'address_input': user_input,
        'distance_matrix_api_response': data_matrix_json,
        'weather_api_response': tomorrow_io_info,
        'statistics':{'date':datetime.datetime.now().strftime(format="%Y-%m-%d %H:%M:%S")}
    }

    return dict_raw

def _save_raw_data(raw_data_dict):

    address = str(raw_data_dict['distance_matrix_api_response']['result'][0]['formatted_address'])
    date_now = str(datetime.datetime.today()).split(' ')[0]

    hash_id = str(hashlib.sha256(str(f"{address}{date_now}").strip().encode('utf-8')).hexdigest())
    raw_data_dict['hash_id'] = hash_id
    filename = f"{hash_id}.json"
    with open(f"{raw_data_path}/{filename}",'w',encoding='utf-8') as savefile:
        savefile.write(json.dumps(raw_data_dict))

def _extract_data(raw_data):
    try:
        all_dataframes = []
        for data in raw_data['weather_api_response']['timelines']['daily']:
            forecast_data = data['values']
            forecast_data['address'] = str(raw_data['distance_matrix_api_response']['result'][0]['formatted_address'])
            forecast_data['latitude'] = raw_data['distance_matrix_api_response']['result'][0]["geometry"]["location"]['lat']
            forecast_data['longitude'] = raw_data['distance_matrix_api_response']['result'][0]["geometry"]["location"]['lng']
            forecast_data['time'] = data['time']
            df = pd.DataFrame([forecast_data])
            all_dataframes.append(df)

        merged_df = pd.concat(all_dataframes)
        return merged_df
    except:
        print(traceback.format_exc())
        return None

def _data_schema(df):
    try:
        for col in df.columns:
            if col in ['address', 'hash_id']:
                df[col] = df[col].astype(str)
            elif col in ['time','data_collected']:
                df[col] = pd.to_datetime(df[col],errors='coerce')
            else:
                df[col] = pd.to_numeric(df[col],errors='coerce')

        return df
    except:
        print(traceback.format_exc())
        return None


def collect_data():
    user_input = 'sao lucas sao paulo'

    latlon, data_matrix_response = _search_latlon(user_input=user_input)
    weather_result = _search_weather_condition(lat_lon=latlon)

    raw_data_to_save = _generate_raw_json(data_matrix_json=data_matrix_response,
                                          tomorrow_io_info=weather_result,
                                          user_input=user_input)

    _save_raw_data(raw_data_to_save)

def process_data():
    all_data_collected = glob.glob(f"{raw_data_path}/*.json")

    print(all_data_collected)

    for file in all_data_collected:
        with open(file, 'r', encoding='utf-8') as openfile:
            file_content = json.load(openfile)

        extracted_data = _extract_data(raw_data=file_content)
        if extracted_data is not None and isinstance(extracted_data, pd.DataFrame):
            validated_data = _data_schema(extracted_data)
            if validated_data is not None and isinstance(validated_data, pd.DataFrame):
                validated_data['hash_id'] = file_content['hash_id']
                filename = f"{file_content['hash_id']}.parquet"
                validated_data.to_parquet(f"{processed_data_path}/{filename}", index=False)

def data_partitioning():
    all_staging_files = glob.glob(f"{processed_data_path}/*.parquet")
    print(all_staging_files)

    for file in all_staging_files:
        df = pd.read_parquet(file)
        print(df['address'])
        address_list = list(set(df['address'].unique().tolist()))
        print(address_list)
        for address in address_list:
            country = str(address.split(',')[-1]).strip()
            try:
                state = str(address.split(',')[-2].split('-')[1]).split('State of')[1].strip()
            except IndexError:
                state = str(address.split(',')[-2].split('-')[1]).strip()

            city = str(address.split(',')[-2].split('-')[0]).strip()
            region = str(address.split(',')[0]).strip()

            partitioned_path = partitioned_data_path.joinpath(country).joinpath(state).joinpath(city).joinpath(region)
            partitioned_path.mkdir(parents=True, exist_ok=True)
            df_filtered = df[df['address'] == address]

            unique_id = str(df_filtered['hash_id'].unique().tolist()[0])

            df_filtered.to_parquet(f"{partitioned_path}/{unique_id}.parquet", index=False)


if __name__ == '__main__':
    collect_data()
    process_data()
    data_partitioning()