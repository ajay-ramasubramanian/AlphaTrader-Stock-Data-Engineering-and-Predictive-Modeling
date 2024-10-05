from kafka import KafkaProducer
from base_producer import SpotifyKafkaProducer
import os
from datetime import datetime
from utils import scope
import pandas as pd
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()
class FollowingArtistsProducer(SpotifyKafkaProducer):
    def __init__(self, ):
        super().__init__()


    def process_spotify_data(self, user_id):
        """
        Processes Spotify data for the given user by retrieving their followed artists 
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []  # List to keep track of future objects for asynchronous Kafka sends

        try:
            after = None  # Cursor for pagination in Spotify API
            limit = 1  # Limit for number of items to fetch per request (can be adjusted)

            while True:
                # Fetch the current user's followed artists with pagination support
                result = self.sp.current_user_followed_artists(limit=limit, after=after)
                
                # Send the data to Kafka as soon as it is retrieved
                future = self.produce_following_artists(user_id, result)
                futures.append(future)
                

                # Check if there is a next page of results; if not, exit the loop
                if result['artists']['next']:
                    after = result['artists']['cursors']['after']  # Update cursor for next request
                else:
                    break

            # Wait for all Kafka messages to be sent and handle their results
            for future in futures:
                try:
                    kafka_future = future.result()  # Wait for the send to complete
                    record_metadata = kafka_future.get(timeout=10)  # Retrieve Kafka metadata
                    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                except Exception as e:
                    print(f"Failed to send message: {e}")

        finally:
            # Close the producer to release resources
            self.close()

def run_producer_following_artists():
    following_artists = FollowingArtistsProducer()
    following_artists.process_spotify_data(os.getenv('USER_NAME'))


if __name__ == "__main__":
    # Start the data processing for a specific user
    run_producer_following_artists()
