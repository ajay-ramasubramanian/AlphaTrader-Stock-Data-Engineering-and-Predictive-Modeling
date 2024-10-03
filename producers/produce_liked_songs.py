import os
from datetime import datetime
import pandas as pd
from spotipy import Spotify
import time
import spotipy
from dotenv import load_dotenv
from kafka import KafkaProducer
from spotipy.oauth2 import SpotifyOAuth
from produce_related_artists import RelatedArtistsProducer
from produce_artist_albums import ArtistAlbumsProducer


from base_producer import SpotifyKafkaProducer
from utils import scope

# Load environment variables from .env file (if needed)
load_dotenv()
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
        super().__init__()
        # Initialize Kafka producer from base class

    def send_ids_to_related_artists_producer(self, user_id, artist_ids):
        """
        Send artist and album IDs to another file for processing related data.
        
        Args:
            artist_ids (list): List of artist IDs.
            album_ids (list): List of album IDs.
        """
        RelatedArtistsProducer().get_artist_ids(user_id, artist_ids)

    def send_ids_to_artist_albums_producer(self, user_id, artist_ids):
        """
        Send artist and album IDs to another file for processing related data.
        
        Args:
            artist_ids (list): List of artist IDs.
            album_ids (list): List of album IDs.
        """
        print(artist_ids)
        ArtistAlbumsProducer().get_artist_ids(user_id, artist_ids)


    def process_spotify_data(self, user_id):
        """
        Processes Spotify data for the given user by retrieving their saved tracks 
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []  # List to keep track of future objects for asynchronous Kafka sends
        max = 300

        try:
            offset = 0  # Offset for pagination in Spotify API
            limit = 1  # Limit for the number of items to fetch per request (can be adjusted)
            artist_ids = []
            # Fetch the current user's saved tracks with pagination support
            while True:
                # Fetch the current user's saved tracks with pagination support
                result = self.sp.current_user_saved_tracks(limit=limit, offset=offset)
                time.sleep(0.2)
                # print(f'results: {result}')
                # break
                
                
                # print("artists_id: ", result['items'][0]['track']['artists'][0]['id'])
                
                # Break the loop if no items are returned
                if not result['items']:
                    break

                # Send the data to Kafka as soon as it is retrieved
                future = self.produce_liked_songs(user_id, result)
                futures.append(future)
                artist_ids.append(result['items'][0]['track']['artists'][0]['id'])
                
                
                # Increment offset for the next batch of items
                offset += limit

            print("Sent all the data")  # Confirmation print
            # After the while loop
            

            # Wait for all Kafka messages to be sent and handle their results
            for future in futures:
                try:
                    kafka_future = future.result()  # Wait for the send to complete
                    record_metadata = kafka_future.get(timeout=10)  # Retrieve Kafka metadata
                    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                except Exception as e:
                    print(f"Failed to send message: {e}")
            # print(f'artist id :{artist_ids}')
            if artist_ids:   # artist_ids is a list
                self.send_ids_to_artist_albums_producer(user_id, artist_ids)
                # self.send_ids_to_related_artists_producer(user_id, artist_ids)

        finally:
            # Close the producer to release resources
            self.close()


def run_producer_liked_songs():
    saved_tracks_producer = SavedTracksProducer()
    saved_tracks_producer.process_spotify_data(os.getenv('USER_NAME'))


if __name__ == "__main__":
    # Start the data processing for a specific user
    run_producer_liked_songs()
    
