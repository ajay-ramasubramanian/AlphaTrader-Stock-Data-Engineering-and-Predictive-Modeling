from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import json
from spotify_user_data_extraction import *


# Create a Kafka producer
producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    ) 

def producer():
    
    # Function to send DataFrame as a message
    def send_dataframe(df, topic):
        # Convert DataFrame to dictionary
        data = df.to_dict(orient='records')
        
        # Send the data as a message
        future = producer.send(topic, value=data)
        
        try:
            record_metadata = future.get(timeout=10)
            print(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
        except Exception as e:
            print(f"Error sending message: {e}")

    return send_dataframe



send_df = producer()
send_df(users_saved_tracks, 'quickstart-events')
producer.flush()
producer.close()