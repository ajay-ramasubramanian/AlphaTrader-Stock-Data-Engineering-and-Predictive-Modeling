import os
from datetime import datetime

import pandas as pd
import spotipy
from dotenv import load_dotenv
from kafka import KafkaProducer
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

from base_producer import SpotifyKafkaProducer
from utils import scope

# Load environment variables from .env file (if needed)
# load_dotenv()
# clientID= os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

# Initialize Spotipy client with OAuth credentials and the required scope
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

def process_spotify_data(user_id):
    """
    Processes Spotify data for the given user by retrieving their saved tracks 
    and sending this data to Kafka for downstream processing.

    Args:
        user_id (str): The Spotify user ID.
    """
    # Initialize Kafka producer
    producer = SpotifyKafkaProducer()
    futures = []  # List to keep track of future objects for asynchronous Kafka sends

    try:
        offset = 0  # Offset for pagination in Spotify API
        limit = 1  # Limit for number of items to fetch per request (can be adjusted)

        while True:
            # Fetch the current user's saved tracks with pagination support
            result = sp.current_user_saved_tracks(limit=limit, offset=offset)
            print("..")  # Optional debug print

            # Break the loop if no items are returned
            if not result['items']:
                break

            # Send the data to Kafka as soon as it is retrieved
            future = producer.produce_liked_songs(user_id, result)
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
        producer.close()

if __name__ == "__main__":
    # Start the data processing for a specific user
    process_spotify_data('suhaas')
