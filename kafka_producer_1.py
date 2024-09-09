import json

import pandas as pd
from kafka import KafkaProducer

from utils.process_spotify_api import (get_saved_tracks_as_dataframe,
                                       get_user_top_artists)

# Create a Kafka producer
kafka_producer = KafkaProducer(
        bootstrap_servers='localhost:9093',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def producer(api_call, topic):
    
    df = api_call()
    # Convert DataFrame to dictionary
    data = df.to_dict(orient='records')
    
    # Send the data as a message
    future = kafka_producer.send(topic, key=b'suhaas', value=data, partition=3)
    
    try:
        record_metadata = future.get(timeout=10)
        print(f"Message sent to topic {record_metadata.topic},\
              partition {record_metadata.partition}, \
              offset {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {e}")


# change topic name
producer(get_user_top_artists, 'dataframe-topic')
kafka_producer.flush()
kafka_producer.close()