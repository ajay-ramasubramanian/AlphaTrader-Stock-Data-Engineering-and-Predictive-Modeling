from kafka import KafkaProducer
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime
import time
import pandas as pd
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

# load_dotenv()
# clientID= os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

scope = "user-library-read \
         user-follow-read \
         playlist-read-private \
         playlist-read-collaborative \
         user-top-read \
         user-read-recently-played \
         playlist-modify-public \
         playlist-modify-private \
         user-read-private \
         user-read-email"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']

TOPICS = {
    'following_artists': 'spotify_following_artists',
    'liked_songs': 'spotify_liked_songs',
    'recent_plays': 'spotify_recent_plays',
    'saved_playlists': 'spotify_saved_playlists',
    'top_artists': 'spotify_top_artists',
    'top_songs': 'spotify_top_songs'
}

class SpotifyKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=str.encode,
            batch_size=16384,
            linger_ms=100,
            compression_type='gzip'
        )
        self.executor = ThreadPoolExecutor(max_workers=5)  # Adjust based on your needs

    def produce_message(self, topic_key, user_id, data):
        if topic_key not in TOPICS:
            raise ValueError(f"Invalid topic: {topic_key}")
        
        topic = TOPICS[topic_key]
        future = self.producer.send(topic, key=user_id, value=data)
        return future

    def produce_following_artists(self, user_id, track_data):
        return self.executor.submit(self.produce_message, 'following_artists', user_id, track_data)

    # def produce_album(self, user_id, album_data):
    #     return self.executor.submit(self.produce_message, 'albums', user_id, album_data)

    # def produce_artist(self, user_id, artist_data):
    #     return self.executor.submit(self.produce_message, 'artists', user_id, artist_data)

    # def produce_playlist(self, user_id, playlist_data):
    #     return self.executor.submit(self.produce_message, 'playlists', user_id, playlist_data)

    # def produce_user_profile(self, user_id, profile_data):
    #     return self.executor.submit(self.produce_message, 'user_profile', user_id, profile_data)

    # def produce_listening_history(self, user_id, history_data):
    #     return self.executor.submit(self.produce_message, 'listening_history', user_id, history_data)

    def close(self):
        self.executor.shutdown()
        self.producer.flush()
        self.producer.close()

# Example usage
def process_spotify_data(user_id):
    producer = SpotifyKafkaProducer()
    futures = []

    try:
        after = None
        limit = 1  # Maximum allowed by the API

        while True:
            result = sp.current_user_followed_artists(limit=limit, after=after)
            print("sent 1 record")
            # Send to Kafka as soon as we have the data
            future = producer.produce_following_artists(user_id, result)
            futures.append(future)

            if result['artists']['next']:
                after = result['artists']['cursors']['after']
            else:
                break
        
            # print(f"Retrieved {len(artists)} artists so far...")
        print("Sent all the data")
            
        

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