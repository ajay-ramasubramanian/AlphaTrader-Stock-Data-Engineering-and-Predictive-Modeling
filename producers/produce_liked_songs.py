import os
from datetime import datetime
import pandas as pd
import spotipy
from dotenv import load_dotenv
from kafka import KafkaProducer
from spotipy.oauth2 import SpotifyOAuth

from base_producer import SpotifyKafkaProducer
from utils import scope

# Load environment variables from .env file (if needed)
# load_dotenv()
# clientID = os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

class SavedTracksProducer(SpotifyKafkaProducer):
    """
    A producer class for sending a user's saved Spotify tracks to a Kafka topic.
    """

    def __init__(self):
        """
        Initialize the SavedTracksProducer with a Spotify client and Kafka producer.
        """
        super().__init__()  # Initialize Kafka producer from base class
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))  # Initialize Spotipy client

    def process_spotify_data(self, user_id):
        """
        Processes Spotify data for the given user by retrieving their saved tracks 
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []  # List to keep track of future objects for asynchronous Kafka sends

        try:
            offset = 0  # Offset for pagination in Spotify API
            limit = 1  # Limit for the number of items to fetch per request (can be adjusted)

            while True:
                # Fetch the current user's saved tracks with pagination support
                result = self.sp.current_user_saved_tracks(limit=limit, offset=offset)
                # print("..")  # Optional debug print
                
                # Break the loop if no items are returned
                if not result['items']:
                    break

                # Send the data to Kafka as soon as it is retrieved
                future = self.produce_liked_songs(user_id, result)
                futures.append(future)
                
                
                # Increment offset for the next batch of items
                offset += limit

            print("Sent all the data")  # Confirmation print

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

if __name__ == "__main__":
    # Start the data processing for a specific user
    saved_tracks_producer = SavedTracksProducer()
    saved_tracks_producer.process_spotify_data('suhaas')
