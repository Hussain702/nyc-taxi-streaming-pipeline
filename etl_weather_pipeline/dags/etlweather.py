
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.sensors.http import HttpSensor

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

# Constants
POSTGRES_CONN_ID = 'Postgres_default'
API_CONN_ID = 'open_meteo_api'
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'


@dag(
    start_date=datetime(2025, 7, 22),  # or datetime.today() if you want it dynamic
    schedule="@daily",
    catchup=False,
    tags=["weather", "ETL"]
)
def weather_etl_pipeline():


    wait_for_api=HttpSensor(
        task_id='wait_for_weather_api',
        http_conn_id=API_CONN_ID,
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true',
        poke_interval=10,
        timeout=60,
        mode='poke'

    ) 

    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")


       


    @task()
    def transform_weather_data(weather_data):
        current = weather_data["current_weather"]
        return {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current["temperature"],
            "windspeed": current["windspeed"],
            "winddirection": current["winddirection"],
            "weathercode": current["weathercode"]
        }

    @task()
    def load_weather_data(data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.DataFrame([data])  # wrap in list to make valid DataFrame
        df.to_sql(
            name='weather_data',
            con=engine,
            if_exists='append',  # use 'append' in production
            index=False,
            schema='public'
        )

    # Define task dependencies
    weather_data = extract_weather_data()
    transformed = transform_weather_data(weather_data)
   
    wait_for_api >>weather_data>>transformed>>load_weather_data(transformed)

# Instantiate DAG
weather_etl_pipeline()

