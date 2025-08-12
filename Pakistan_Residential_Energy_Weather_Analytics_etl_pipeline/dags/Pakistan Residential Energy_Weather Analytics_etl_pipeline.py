from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import pandas as pd
import logging
import zipfile
import io
import os
os.environ["AIRFLOW__PROVIDERS__ODBC__ALLOW_DRIVER_IN_EXTRA"] = "True"


@dag(
    dag_id='pakistan_energy_weather_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["energy", "weather", "azure"]
)
def Pakistan_Residential_Energy_Weather_Analytics_etl_pipeline():

    @task()
    def extract_energy_weather_data():
        log = logging.getLogger(__name__)
        http_hook = HttpHook(http_conn_id='lums_energy_http', method='GET')
        wasb_hook = WasbHook(wasb_conn_id='azure_datalake_conn')

        energy_response = http_hook.run(endpoint='/~eig/gallery/datasets/rewdp/rewdp_dataset.zip')
        weather_response = http_hook.run(endpoint='/~eig/gallery/datasets/rewdp/weather_dataset.zip')

        blob_service_client = wasb_hook.get_conn()
        container_client = blob_service_client.get_container_client('raw')

        try:
            container_client.upload_blob(
                name='raw-data/energy-data.zip',
                data=energy_response.content,
                overwrite=True
            )
            log.info("Energy data uploaded.")
        except Exception as e:
            log.warning(e)

        try:
            container_client.upload_blob(
                name='raw-data/weather-data.zip',
                data=weather_response.content,
                overwrite=True
            )
            log.info("Weather data uploaded.")
        except Exception as e:
            log.warning(e)

    @task()
    def transform_energy_weather_data():
        wasb_hook=WasbHook(wasb_conn_id='azure_datalake_conn')
        blob_service_client=wasb_hook.get_conn()
        container_client=blob_service_client.get_container_client('raw')
        energy_zip_name='raw-data/energy-data.zip'
        weather_zip_name = "raw-data/weather-data.zip"


        def read_zip_from_lake(zip_name):
            blob_client=container_client.get_blob_client(zip_name)
            stream=io.BytesIO()
            blob_client.download_blob().readinto(stream)
            stream.seek(0)
            return zipfile.ZipFile(stream,'r')

        #Transform Energy Data
        energy_chunks=[]
        energy_ref=read_zip_from_lake(energy_zip_name)

        for file_name in energy_ref.namelist():
            if file_name.endswith('.csv') and "Islamabad" in file_name:
                with energy_ref.open(file_name) as f:
                    for chunk in pd.read_csv(f,chunksize=20000):
                        if 'datetime' in chunk.columns:
                            chunk['City']="Islamabad"
                            chunk['datetime']=pd.to_datetime(chunk['datetime'],errors='coerce')
                            chunk.dropna(subset=['datetime'],inplace=True)
                            chunk.dropna(axis=1,how='all',inplace=True)
                            chunk.columns=[col.strip().replace('/n','').replace('/r','') for col in chunk.columns]
                            energy_chunks.append(chunk)
        energy_df=pd.concat(energy_chunks,ignore_index=True)   
        necessary_energy_cols = [
            'datetime', 'City', 'Usage (kW)', 'FF (kW)', 'DR_AC (kW)', 'BR_AC (kW)',
            'Kitchen (kW)', 'Water Pump_1 (kW)', 'Geyser (kW)', 'UPS + Fridge (kW)',
            'Washing Machine (kW)', 'Fridge (kW)', 'Lights + Fan (kW)', 'UPS (kW)'
        ]        
        energy_df=energy_df[[col for col in necessary_energy_cols if col in energy_df.columns]]       


  

        #Transform Weather data
        weather_chunks=[]
        weather_ref=read_zip_from_lake(weather_zip_name)

        for file_name in weather_ref.namelist():
            if file_name.endswith('.csv') and "Islamabad" in file_name:
                with weather_ref.open(file_name) as f:
                    for chunk in pd.read_csv(f,chunksize=20000):
                        if 'datetime' in chunk.columns:
                            chunk['City']="Islamabad"
                            chunk['datetime']=pd.to_datetime(chunk['datetime'],errors='coerce')
                            chunk.dropna(subset=['datetime'],inplace=True)
                            chunk.dropna(axis=1,how='all',inplace=True)
                            chunk.columns=[col.strip().replace('/n','').replace('/r','') for col in chunk.columns]
                            weather_chunks.append(chunk)
        weather_df=pd.concat(weather_chunks,ignore_index=True)    
        basic_weather_cols = ['datetime', 'City', 'Temperature', 'Humidity', 'Wind Speed','Pressure']
        weather_df = weather_df[[col for col in basic_weather_cols if col in weather_df.columns]]
       

        combined_df=pd.merge(
            energy_df,
            weather_df,
            on=['datetime','City'],
            how='inner'
        )
        combined_df.sort_values(by='datetime',inplace=True)
        combined_df.reset_index(drop=True,inplace=True)
        

        combined_df = combined_df.rename(columns={

            'datetime':'d_id',
            'City':'city',
            'Usage (kW)': 'usage_kw',
            'FF (kW)': 'ff_kw',
            'DR_AC (kW)': 'dr_ac_kw',
            'BR_AC (kW)': 'br_ac_kw',
            'Kitchen (kW)': 'kitchen_kw',
            'Water Pump_1 (kW)': 'water_pump_kw',
            'Geyser (kW)': 'geyser_kw',
            'Washing Machine (kW)': 'washing_machine_kw',
            'Fridge (kW)': 'fridge_kw',
            'UPS (kW)': 'ups_kw',
            'Temperature': 'temperature',
            'Humidity': 'humidity',
            'Wind Speed': 'wind_speed',
            'Pressure': 'pressure'
        })

        output_csv = combined_df.to_csv(index=False)
        

        container_client.upload_blob(
            name='Transformed-data/Islamabad.csv',
            data=output_csv,
            overwrite=True
            

        )
       

    @task()
    def load_to_synapse():
        hook = OdbcHook(
          odbc_conn_id="azure_synapse_odbc_conn",
          driver="{ODBC Driver 18 for SQL Server}"  # force driver explicitly
        )

        conn = hook.get_conn()
        cursor = conn.cursor()
    
        cursor.execute("""
          COPY INTO staging_energy_usage
          FROM 'https://datatricksexternal.dfs.core.windows.net/raw/Transformed-data/Islamabad.csv'
          WITH (
              FILE_TYPE = 'CSV',
              CREDENTIAL = (IDENTITY = 'Managed Identity'),
              FIELDTERMINATOR = ',',
              ROWTERMINATOR = '0x0A',
              FIRSTROW = 2
            );

        """)
    
        conn.commit()
        cursor.close()
        conn.close()

    #transform_energy_weather_data()>>
    load_to_synapse()

Pakistan_Residential_Energy_Weather_Analytics_etl_pipeline()
