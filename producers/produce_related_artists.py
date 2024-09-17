import os
from datetime import datetime
import pandas as pd
from spotipy import Spotify
import time
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

class RelatedArtistsProducer(SpotifyKafkaProducer):
    """
    A producer class for sending a artists related to artists followed by the user to a Kafka topic.
    """

    def __init__(self):
        """
        Initialize the SavedTracksProducer with a Spotify client and Kafka producer.
        """
        super().__init__()

    def get_artist_ids(self, artist_ids):
        self.process_spotify_data(artist_ids)

    def get_related_artists(self, sp, artist_id):
        try:
            related = sp.artist_related_artists(artist_id)
            return related['artists']
        except spotipy.SpotifyException as e:
            print(f"Error getting related artists for {artist_id}: {e}")
            return []


    def process_spotify_data(self, artist_ids=None, depth=2, max_artists=100):
        """
        Processes Spotify data for the given user by retrieving their saved tracks 
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []  # List to keep track of future objects for asynchronous Kafka sends
        artist_ids = ['4IHSCHg3UPSy0rBSHi3c5s', '7Hjbimq43OgxaBRpFXic4x', '3PWp9R5HvbQgxI5KBx5kVd', '1t17z3vfuc82cxSDMrvryJ', '4EPYWwU4c8eG2GzD7MenUA', '6PDLwWvgYNMfBRLqC1h5cJ', '2dixWDh9f2COEfikojSd39']
        try:
            artist_set = set()
            to_process = set(artist_ids)
            processed = set()
            c = 0

            for _ in range(depth):
                current_level = set()
                for artist_id in to_process:
                    if artist_id not in processed:
                        time.sleep(1)
                        related = self.get_related_artists(self.sp, artist_id)
                        for artist in related: # take name, id, followers, genres
                            if len(artist_set) < max_artists:
                                c += 1
                                # print(f"artist: {artist}")
                                id = artist['id']
                                name = artist['name']
                                number_of_followers = artist['followers']['total']
                                genres = tuple(artist['genres'])
                                popularity = artist['popularity']
                                image = artist['images'][0]['url']
                                artist_set.add((id, name, number_of_followers, genres, popularity, image))
                                current_level.add(artist['id'])
                                # print(f"artist: {artist}")
                        processed.add(artist_id)
                    if len(artist_set) >= max_artists:
                        break
                    time.sleep(0.1)  # Add a small delay to respect rate limits
                to_process = current_level
                print(artist_set)
                if len(artist_set) >= max_artists:
                    break

            
            print(f"length of set: {len(artist_set)}")
            print(f"total artists: {c}")
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
    related_artists_producer = RelatedArtistsProducer()
    related_artists_producer.process_spotify_data()
