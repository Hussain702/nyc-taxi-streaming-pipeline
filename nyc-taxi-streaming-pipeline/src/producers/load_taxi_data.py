import json
import time
import pandas as pd
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data, default=str).encode('utf-8')  

server = 'localhost:9092'
topic_name = 'taxi-data'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)
t0 = time.time()
if __name__ == '__main__':
    df = pd.read_csv('data.csv')

    # Keep only required columns
    df = df[['lpep_pickup_datetime',
             'lpep_dropoff_datetime','PULocationID',
             'DOLocationID','passenger_count',
             'trip_distance','tip_amount']]

    # Convert datetime columns (optional but good practice)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    
    ready_data = df.to_dict(orient='records')

  
    for record in ready_data:
        producer.send(topic_name, value=record)
        print(f"Sent: {record}")
        time.sleep(0.5)  # optional, to mimic real-time streaming

    producer.flush()
    producer.close()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')