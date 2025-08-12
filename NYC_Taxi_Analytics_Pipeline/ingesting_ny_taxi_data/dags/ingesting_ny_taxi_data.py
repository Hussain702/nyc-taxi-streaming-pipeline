from airflow.decorators import dag,task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
# from airflow.providers.odbc.hooks.odbc import OdbcHook
from datetime import datetime
import logging
import pandas as pd
import os 
os.environ["AIRFLOW__PROVIDERS__ODBC__ALLOW_DRIVER_IN_EXTRA"] = "True"
ny_taxi_link='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'

@dag(
    dag_id='ingesting_ny_taxi_data',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
)

def ingesting_ny_taxi_data ():
    @task()
    def extract_ny_taxi_data():
        http_hook=HttpHook(http_conn_id='http_ny_taxi', method='GET')
        endpoint='/trip-data/yellow_tripdata_2025-01.parquet'
        response=http_hook.run(endpoint)
        return response.content

    @task()
    def load_to_lake(ny_data):
        log=logging.getLogger(__name__)
        wasb_hook=WasbHook(wasb_conn_id='azure_datalake_conn')
        blob_service_client=wasb_hook.get_conn()
        container_client=blob_service_client.get_container_client('raw')
        
        try:
            container_client.upload_blob(
               name='ny_taxi/ny_taxi_data.parquet',
               data=ny_data,
               overwrite=True
            )
        except Exception as e:
            log.warning(e)



    extracted_data = extract_ny_taxi_data()
    load_to_lake(extracted_data)

ingesting_ny_taxi_data()    
            


