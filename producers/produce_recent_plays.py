from kafka import KafkaProducer
from base_producer import SpotifyKafkaProducer
import os
import time
from datetime import datetime
from utils import scope
import pandas as pd
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from datetime import datetime
import calendar


class RecentlyPlayedProducer(SpotifyKafkaProducer):
    def __init__(self):
        super().__init__()

    def convert_to_unix_timestamp(self, played_at):

        # Parse the timestamp string to a datetime object
        dt = datetime.strptime(played_at, "%Y-%m-%dT%H:%M:%S.%fZ")

        # Convert to UTC timestamp in seconds
        utc_timestamp = calendar.timegm(dt.utctimetuple())

        # Convert to milliseconds
        milliseconds = int(utc_timestamp * 1000 + dt.microsecond / 1000) - 1

        return milliseconds

        

    def process_spotify_data(self, user_id):
        """
        Processes Spotify data for the given user by retrieving their recently played tracks
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []
        try:
            after =None
            limit = 50
            track_count = 50
            max_tracks = 100

            result = self.sp.current_user_recently_played(limit=limit)
            # print(result)
            # Send to Kafka as soon as we have the data
            future = self.produce_recent_plays(user_id, result)
            futures.append(future)
            # print(f"Sent {track_count} tracks")

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

def run_producer_recent_plays():
    recent_plays = RecentlyPlayedProducer()
    recent_plays.process_spotify_data(os.getenv('USER_NAME'))


if __name__ == "__main__":
    run_producer_recent_plays()

