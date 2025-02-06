import requests
import pandas as pd
import json
import datetime
from pathlib import Path
import os
import hashlib

# Creating path to local files
project_path = Path(os.getcwd()).joinpath('data_weather_app')
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

def search_latlon(user_input:str):
    latlon = None
    dis_matrix_api = '52E2Gs71uO7V5aoZkYVsO0Dc2iMiBbnSh2AphRq8ksGQohOGMOP0OISg0BK8uEWD'
    distance_matrix = f"https://api.distancematrix.ai/maps/api/geocode/json?address={user_input}&key={dis_matrix_api}"

    # response = requests.get(distance_matrix)
    # print(response.text)
    response_json = {"result":[{"address_components":[{"long_name":"vila prudente","short_name":"vila prudente","types":["suburb"]},{"long_name":"são","short_name":"são","types":["locality"]},{"long_name":"paulo state of são","short_name":"paulo state of são","types":["house"]},{"long_name":"paulo","short_name":"paulo","types":["locality"]},{"long_name":"brazil","short_name":"brazil","types":["country"]}],"formatted_address":"Vila Prudente, São Paulo - State of São Paulo, Brazil","geometry":{"location":{"lat":-23.57966035,"lng":-46.5838581},"location_type":"APPROXIMATE","viewport":{"northeast":{"lat":-23.57966035,"lng":-46.5838581},"southwest":{"lat":-23.57966035,"lng":-46.5838581}}},"place_id":"","plus_code":{},"types":["locality","political"]}],"status":"OK"}

    response_latlon = response_json['result'][0]["geometry"]["location"]

    return response_latlon, response_json

def search_weather_condition(lat_lon: dict):
    latitude = lat_lon['lat']
    longitude = lat_lon['lng']
    print(latitude, longitude)

    tomorrow_api = 'O6laJOEXh1FVGBBUnaeErwhQjwyMfnem'
    tomorrow_io = f"https://api.tomorrow.io/v4/weather/forecast?location={latitude},{longitude}&apikey={tomorrow_api}"
    response = requests.get(tomorrow_io)
    response = json.loads(response.text)
    return response

def generate_raw_json(user_input,data_matrix_json: dict, tomorrow_io_info: dict):

    dict_raw = {
        'address_input': user_input,
        'distance_matrix_api_response': data_matrix_json,
        'weather_api_response': tomorrow_io_info,
        'statistics':{'date':datetime.datetime.now().strftime(format="%Y-%m-%d %H:%M:%S")}
    }

    return dict_raw

def save_raw_data(raw_data_dict):

    address = str(raw_data_dict['distance_matrix_api_response']['result'][0]['formatted_address'])
    date_now = str(datetime.datetime.today()).split(' ')[0]

    hash_id = str(hashlib.sha256(str(f"{address}{date_now}").strip().encode('utf-8')).hexdigest())

    filename = f"{hash_id}.json"
    with open(f"{raw_data_path}/{filename}",'w',encoding='utf-8') as savefile:
        savefile.write(json.dumps(raw_data_dict))

user_input = 'vila prudente sao paulo'

latlon, data_matrix_response = search_latlon(user_input=user_input)
weather_result = search_weather_condition(lat_lon=latlon)

raw_data_to_save = generate_raw_json(data_matrix_json=data_matrix_response,
                  tomorrow_io_info=weather_result,
                  user_input=user_input)

save_raw_data(raw_data_to_save)

