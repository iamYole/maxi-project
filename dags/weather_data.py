import openmeteo_requests
import logging
import os
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
from google.cloud import storage
from io import StringIO
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# load_dotenv('/opt/airflow/.env')


default_args = {
    'owner': 'Ytech',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


bucket_name = os.getenv("BUCKET_NAME")

# List of 5 different latitude and longitude points
LOCATIONS = [
    {'latitude': 52.52, 'longitude': 13.41, 'name': 'berlin'},
    {'latitude': 40.71, 'longitude': -74.01, 'name': 'new_york'},
    {'latitude': 34.05, 'longitude': -118.25, 'name': 'los_angeles'},
    {'latitude': 48.85, 'longitude': 2.35, 'name': 'paris'},
    {'latitude': 51.51, 'longitude': -0.13, 'name': 'london'}
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_weather_data(location: dict):
    """ Fetch weather data from Open-Meteo API for multiple locations """

    try:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": location['latitude'],
            "longitude": location['longitude'],
            "start_date": "2023-01-01",
            "end_date": "2024-12-31",
            "daily": ["weather_code", "temperature_2m_mean", "temperature_2m_max", "sunrise", "sunset", "wind_speed_10m_max", "daylight_duration", "sunshine_duration", "snowfall_sum"],
            "timezone": "Europe/London"
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]
        # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        # print(f"Elevation {response.Elevation()} m asl")
        # print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
        # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

        # Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        daily_weather_code = daily.Variables(0).ValuesAsNumpy()
        daily_temperature_2m_mean = daily.Variables(1).ValuesAsNumpy()
        daily_temperature_2m_max = daily.Variables(2).ValuesAsNumpy()
        daily_sunrise = daily.Variables(3).ValuesInt64AsNumpy()
        daily_sunset = daily.Variables(4).ValuesInt64AsNumpy()
        daily_wind_speed_10m_max = daily.Variables(5).ValuesAsNumpy()
        daily_daylight_duration = daily.Variables(6).ValuesAsNumpy()
        daily_sunshine_duration = daily.Variables(7).ValuesAsNumpy()
        daily_snowfall_sum = daily.Variables(8).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}

        daily_data["weather_code"] = daily_weather_code
        daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
        daily_data["temperature_2m_max"] = daily_temperature_2m_max
        daily_data["sunrise"] = daily_sunrise
        daily_data["sunset"] = daily_sunset
        daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
        daily_data["daylight_duration"] = daily_daylight_duration
        daily_data["sunshine_duration"] = daily_sunshine_duration
        daily_data["snowfall_sum"] = daily_snowfall_sum

        daily_dataframe = pd.DataFrame(data = daily_data)

        logger.info(f"Successfully created DataFrame with {len(daily_dataframe)} rows for {location['name']}")

        # Save to json
        # Return the DataFrame as JSON for passing between tasks
        return daily_dataframe.to_json(orient='records', date_format='iso')
    except Exception as e:
        logger.error(f"Error fetching weather data for {location['name']}: {str(e)}")
        raise

def save_to_gcs(weather_data_json: str, location_name: str):
    """Save weather data to Google Cloud Storage"""
    try:
        logger.info(f"Saving weather data for {location_name} to GCS")
        
        # Convert JSON back to DataFrame
        df = pd.read_json(StringIO(weather_data_json), orient='records')
        df['date'] = pd.to_datetime(df['date'])
        
        # Generate filename with location name and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{location_name}_daily_weather_{timestamp}.parquet"
        
        # Save to GCS as Parquet
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"weather_data/{filename}")
        
        # Convert DataFrame to Parquet and upload
        parquet_data = df.to_parquet(index=False)
        blob.upload_from_string(parquet_data, content_type='application/octet-stream')
        
        logger.info(f"Weather data for {location_name} saved to GCS as {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error saving weather data for {location_name} to GCS: {str(e)}")
        raise

# Main function to run the pipeline
def extract_weather_data_pipeline(location:dict, **kwargs):
    try:
        # Fetch weather data
        weather_data_json = fetch_weather_data(location)
        
        # Save data to GCS
        save_to_gcs(weather_data_json, location['name'])
    
    except Exception as e:
        logger.error(f"Error processing {location['name']}: {str(e)}")

with DAG(
    'extract_weather_data_pipeline',
    default_args=default_args,
    description='Extract historical weather data from Open-Meteo into GCP',
    schedule='0 0 * * *',
    start_date=datetime(2025,7,20),
    catchup=False,
    tags=['etl','sales', 'extract', 'production']
) as dag:
    for location in LOCATIONS:
        extract_weather_data_task = PythonOperator(
            task_id=f'extract_{location['name']}_weather_data',
            python_callable=extract_weather_data_pipeline,
            op_kwargs={'location': location},  # pass location name to function
        )