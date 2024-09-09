from kafka import KafkaProducer
from producers.base_producer import SpotifyKafkaProducer
import os
from datetime import datetime
from utils import scope
import pandas as pd
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

# load_dotenv()
# clientID= os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

def process_spotify_data(user_id):
    producer = SpotifyKafkaProducer()
    futures = []

    try:
        after = None
        limit = 1

        while True:
            result = sp.current_user_followed_artists(limit=limit, after=after)

            # Send to Kafka as soon as we have the data
            future = producer.produce_following_artists(user_id, result)
            futures.append(future)

            if result['artists']['next']:
                after = result['artists']['cursors']['after']
            else:
                break
            

        # Wait for all messages to be sent
        for future in futures:
            try:
                kafka_future = future.result()
                record_metadata = kafka_future.get(timeout=10)
                print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            except Exception as e:
                print(f"Failed to send message: {e}")

    finally:
        producer.close()

if __name__ == "__main__":
    process_spotify_data('suhaas')