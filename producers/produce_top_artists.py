from kafka import KafkaProducer
from base_producer import SpotifyKafkaProducer
import os
from datetime import datetime
from utils import scope
import pandas as pd
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
load_dotenv()
class TopArtistsProducer(SpotifyKafkaProducer):
    def __init__(self):
        super().__init__()

    def process_spotify_data(self, user_id):
        """
        Processes Spotify data for the given user by retrieving their top artists
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []

        try:
            offset = 0
            time_range = "short_term"
            limit = 1

            while True:
                result = self.sp.current_user_top_artists(time_range=time_range, limit=limit, offset=offset)
                
                if not result['items']:
                    break
                # Send to Kafka as soon as we have the data
                future = self.produce_top_artists(user_id, result)
                futures.append(future)
                

                offset += limit

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
            self.close()


def run_producer_top_artist():
    top_artists = TopArtistsProducer()
    top_artists.process_spotify_data(os.getenv('USER_NAME'))

if __name__ == "__main__":
    run_producer_top_artist()
