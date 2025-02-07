import datetime
import glob
import hashlib
import json
import traceback
from pathlib import Path

import pandas as pd
import requests

# Define project directory structure
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


def _search_latlon(address: str):
    """
        Fetch latitude and longitude for a given address using the Distance Matrix API.

        Args:
            address (str): The address to fetch latitude and longitude for.

        Returns:
            tuple: A dictionary with latitude/longitude and the full API response.
        """
    dis_matrix_api = '52E2Gs71uO7V5aoZkYVsO0Dc2iMiBbnSh2AphRq8ksGQohOGMOP0OISg0BK8uEWD'
    distance_matrix = f"https://api.distancematrix.ai/maps/api/geocode/json?address={address}&key={dis_matrix_api}"

    response = requests.get(distance_matrix)
    response_json = json.loads(response.text)

    response_latlon = response_json['result'][0]["geometry"]["location"]

    return response_latlon, response_json


def _search_weather_condition(lat_lon: dict):
    """
        Fetch weather forecast for a given latitude and longitude using Tomorrow.io API.

        Args:
            lat_lon (dict): A dictionary with 'lat' and 'lng' keys representing coordinates.

        Returns:
            dict: The weather forecast data.
        """
    latitude = lat_lon['lat']
    longitude = lat_lon['lng']
    print(latitude, longitude)

    tomorrow_api = 'O6laJOEXh1FVGBBUnaeErwhQjwyMfnem'
    tomorrow_io = f"https://api.tomorrow.io/v4/weather/forecast?location={latitude},{longitude}&apikey={tomorrow_api}"
    response = requests.get(tomorrow_io)
    response = json.loads(response.text)
    return response


def _generate_raw_json(address_input, geo_data: dict, weather_data: dict):
    """
       Generate a structured raw data dictionary combining geolocation and weather data.

       Args:
           address_input (str): The input address.
           geo_data (dict): Geolocation API response.
           weather_data (dict): Weather API response.

       Returns:
           dict: Combined raw data with a timestamp and hash ID.
    """
    dict_raw = {
        'address_input': address_input,
        'distance_matrix_api_response': geo_data,
        'weather_api_response': weather_data,
        'statistics': {'date': datetime.datetime.now().strftime(format="%Y-%m-%d %H:%M:%S")}
    }

    return dict_raw


def _save_raw_data(raw_data):
    """
        Save the raw data as a JSON file with a unique hash-based filename.

        Args:
            raw_data (dict): The raw data dictionary.
    """
    address = str(raw_data['distance_matrix_api_response']['result'][0]['formatted_address'])
    date_now = str(datetime.datetime.today()).split(' ')[0]

    hash_id = str(hashlib.sha256(str(f"{address}{date_now}").strip().encode('utf-8')).hexdigest())
    raw_data['hash_id'] = hash_id
    filename = f"{hash_id}.json"
    with open(f"{raw_data_path}/{filename}", 'w', encoding='utf-8') as savefile:
        savefile.write(json.dumps(raw_data))


def _extract_data(raw_data):
    """
        Extract weather data from raw JSON and convert it into a DataFrame.

        Args:
            raw_data (dict): Raw JSON data containing weather information.

        Returns:
            pd.DataFrame: Extracted and flattened weather data.
        """
    try:
        all_dataframes = []
        for forecast_type in list(raw_data['weather_api_response']['timelines'].keys()):
            data = raw_data['weather_api_response']['timelines'][forecast_type]
            forecast_data = data['values']
            forecast_data['address'] = str(raw_data['distance_matrix_api_response']['result'][0]['formatted_address'])
            forecast_data['latitude'] = raw_data['distance_matrix_api_response']['result'][0]["geometry"]["location"][
                'lat']
            forecast_data['longitude'] = raw_data['distance_matrix_api_response']['result'][0]["geometry"]["location"][
                'lng']
            forecast_data['time'] = data['time']
            df = pd.DataFrame([forecast_data])
            all_dataframes.append(df)

        merged_df = pd.concat(all_dataframes)
        return merged_df
    except:
        print(traceback.format_exc())
        return None


def _data_schema(df):
    """
        Validate and convert DataFrame columns to the appropriate data types.

        Args:
            df (pd.DataFrame): DataFrame to be validated.

        Returns:
            pd.DataFrame: Validated DataFrame.
        """
    try:
        for col in df.columns:
            if col in ['address', 'hash_id']:
                df[col] = df[col].astype(str)
            elif col in ['time', 'data_collected']:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df
    except:
        print(traceback.format_exc())
        return None


def collect_data(location_input: str = None):
    """
    Responsible for collecting data from initial geolocation sources.

    :param location_input: Address to be searched.
    :return:
    """
    if location_input is None:
        user_input = 'vila prudente sao paulo'
    else:
        user_input = location_input

    latlon, data_matrix_response = _search_latlon(address=user_input)
    weather_result = _search_weather_condition(lat_lon=latlon)

    raw_data_to_save = _generate_raw_json(geo_data=data_matrix_response,
                                          weather_data=weather_result,
                                          address_input=user_input)

    _save_raw_data(raw_data_to_save)


def process_data():
    """
    Responsible for process all collected data.
    :return:
    """
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
    """
    Process responsible for partitioning data locally.
    :return:
    """
    all_staging_files = glob.glob(f"{processed_data_path}/*.parquet")
    print(all_staging_files)

    for file in all_staging_files:
        try:
            df = pd.read_parquet(file)
            print(df['address'])
            address_list = list(set(df['address'].unique().tolist()))
            print(address_list)
            for address in address_list:
                country = str(address.split(',')[-1]).strip()
                try:
                    state = str(address.split(',')[-2].split('-')[1]).split('State of')[1].strip()
                except IndexError:
                    try:
                        state = str(address.split(',')[-2].split('-')[1]).strip()
                    except IndexError:
                        state = str(address.split(',')[1].split('-')[1]).strip()

                city = str(address.split(',')[-2].split('-')[0]).strip()
                region = str(address.split(',')[0]).strip()

                partitioned_path = partitioned_data_path.joinpath(country).joinpath(state).joinpath(city).joinpath(
                    region)
                partitioned_path.mkdir(parents=True, exist_ok=True)
                df_filtered = df[df['address'] == address]

                unique_id = str(df_filtered['hash_id'].unique().tolist()[0])

                df_filtered.to_parquet(f"{partitioned_path}/{unique_id}.parquet", index=False)
        except:
            print(traceback.format_exc())


if __name__ == '__main__':
    default_locations = ['Vila Prudente são paulo', 'mocca sao paulo', 'vila clementino sao paulo',
                         'ipiranga sao paulo', 'tamanduatei sao paulo']

    for location in default_locations:
        collect_data(location_input=location)
        process_data()
        data_partitioning()
