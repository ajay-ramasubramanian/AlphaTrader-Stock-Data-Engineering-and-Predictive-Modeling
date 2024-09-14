from kafka import KafkaProducer
from base_producer import SpotifyKafkaProducer
import os
from datetime import datetime
from utils import scope
import pandas as pd
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

class RecentlyPlayedProducer(SpotifyKafkaProducer):
    def __init__(self):
        super().__init__()

    def process_spotify_data(self, user_id):
        """
        Processes Spotify data for the given user by retrieving their recently played tracks
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []
        try:
            before = int(datetime.now().timestamp() * 1000)
            limit = 10
            track_count = 0
            max_tracks = 100

            while True:
                result = self.sp.current_user_recently_played(limit=limit, before=before)
                
                # print(result)
                # break
                if not result['items']:
                    break

                # Send to Kafka as soon as we have the data
                future = self.produce_recent_plays(user_id, result)
                futures.append(future)
                

                played_at = datetime.strptime(result['items'][0]['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                before = int(played_at.timestamp() * 1000)
                track_count += 1
                print(f"Processed track {track_count}")

            print(f"Sent {track_count} tracks")

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

if __name__ == "__main__":
    recent_plays = RecentlyPlayedProducer()
    recent_plays.process_spotify_data('suhaas')
