import json

import pandas as pd
from kafka import KafkaProducer

from utils.process_spotify_api import (get_saved_tracks_as_dataframe,
                                       get_user_followed_artists,
                                       get_user_playlists,
                                       get_user_recently_played_tracks,
                                       get_user_top_artists,
                                       get_user_top_tracks)

# Create a Kafka producer
kafka_producer = KafkaProducer(
        bootstrap_servers='localhost:9093',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def producer(api_call, topic):
    
    df = api_call()
    # Convert DataFrame to dictionary
    # data = df.to_dict(orient='records')
    
    # Send the data as a message
    # print("here")
    print("len of data: ", len(df))
    for i in range(len(df)):
        future = kafka_producer.send(topic, key=b'suhaas', value=df[i:i+10]) 
    
        try:
            record_metadata = future.get(timeout=10)
            print(f"Message sent to topic {record_metadata.topic},\
                partition {record_metadata.partition}, \
                offset {record_metadata.offset}")
        except Exception as e:
            print(f"Error sending message: {e}")
                                       
# change topic name
producer(get_saved_tracks_as_dataframe, 'dataframe-topic')
# producer(get_user_followed_artists, 'dataframe-topic')
# producer(get_user_playlists, 'dataframe-topic')
# # producer(get_user_recently_played_tracks, 'dataframe-topic')
# producer(get_user_top_artists, 'dataframe-topic')
# producer(get_user_top_tracks, 'dataframe-topic')

kafka_producer.flush()
kafka_producer.close()