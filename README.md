# Weather Data Pipeline

This project is a data pipeline that collects, processes, and partitions weather data for various locations. It integrates with external APIs for geocoding and weather forecasting, saving the results as structured data files for further analysis. The pipeline is orchestrated using **Apache Airflow**.

## Features
- **Geocoding**: Converts addresses to latitude/longitude using the Distance Matrix API.  
- **Weather Data Collection**: Fetches weather forecasts from Tomorrow.io for the specified locations.  
- **Data Processing**: Transforms raw API responses into structured data.  
- **Partitioning**: Organises processed data into a hierarchical folder structure by country, state, city, and region.  
- **Airflow Integration**: Automates and schedules the data pipeline.  

#### ⭐Streamlit dashboard 
<img src="https://github.com/user-attachments/assets/cece0e1a-6a1d-4318-a1d1-12a87c8f9517" width="50%" height="50%"/>

#### ⭐Simple Execution
<img src="https://github.com/user-attachments/assets/719cbada-1282-4f55-9ac3-dee5cd226a11" width="50%" height="50%"/>


## Prerequisites
Before running the project, ensure you have the following installed:
- Python 3.x  
- Apache Airflow  
- `requests`, `pandas`,streamlit, apache-airflow, `pyarrow` or `fastparquet` 

## Installation
1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/weather-data-pipeline.git
    cd weather-data-pipeline
    ```
2. Install dependencies:
    ```sh
    pip install .
    ```

3. Configure Airflow.

## Project Structure
```
aiflow_application/
  ├── dags/
  │   ├── weather_app_dag.py      # Airflow DAG for orchestrating the data pipeline
  │   └── data_weather_app/
  │       ├── raw_data/           # Raw JSON data from the APIs
  │       ├── processed_data/     # Processed data in Parquet format
  │       └── partitioned_data/   # Partitioned data by location
  │           └── Brazil/
  │               └── São Paulo/
  │                   └── São Paulo/
  │                       └── Mooca/
  │                           └── a921077759c534251fb9938e3f24e159bea540048dee7494a71902a9335587af.parquet
  └── README.md                   # Project documentation
```

## How to Run
1. After installing airflow, set AIRFLOW_HOME to **airflow_application** path. ```export AIRFLOW_HOME=~/airflow```
2. Initialize airflow with ```airflow webserver```.
3. Ensure you have valid API keys for both the Distance Matrix API and Tomorrow.io. Assign them to `dis_matrix_api` and `tomorrow_api` respectively. (The api keys presented on code documentation will expire soon)
4. Run Airflow and trigger the DAG

   **or ...**
   
5. Run **aiflow_application/dags/weather_application.py**

The pipeline will:
- Collect data for the specified locations.
- Process and clean the data.
- Partition the data into a location-based folder structure.

## Functions Overview
- **`collect_data(location_input: str)`**: Collects weather data for a given location.  
- **`process_data()`**: Processes raw data, validates the schema, and saves it in Parquet format.  
- **`data_partitioning()`**: Partitions processed data into a folder hierarchy based on location.  

## Example
```
Default Locations: Vila Prudente São Paulo, Mooca São Paulo, Vila Clementino São Paulo, Ipiranga São Paulo, Tamanduateí São Paulo
```

Running the DAG will create the following directory structure:
```
partitioned_data/
  └── Brazil/
      └── São Paulo/
          └── São Paulo/
              ├── Vila Prudente/
              │   └── <unique_id>.parquet
              └── Mooca/
                  └── a921077759c534251fb9938e3f24e159bea540048dee7494a71902a9335587af.parquet
```

## Error Handling
The pipeline uses `try-except` blocks to catch and log errors during data extraction, validation, and partitioning, ensuring the process is robust.
