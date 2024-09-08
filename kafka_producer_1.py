import json

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

from utils.process_spotify_api import (recently_played_tracks,
                                       users_saved_tracks)

# Create a Kafka producer
kafka_producer = KafkaProducer(
        bootstrap_servers='localhost:9093',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def producer(fun, topic):
    
    # Function to send DataFrame as a message
    # Convert DataFrame to dictionary
    df = fun()
    data = df.to_dict(orient='records')
    
    # Send the data as a message
    future = kafka_producer.send(topic, key=b'suhaas', value=data, partition=1)
    
    try:
        record_metadata = future.get(timeout=10)
        print(f"Message sent to topic {record_metadata.topic},\
              partition {record_metadata.partition}, \
              offset {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {e}")


# change topic name
producer(users_saved_tracks, 'dataframe-topic')
kafka_producer.flush()
kafka_producer.close()