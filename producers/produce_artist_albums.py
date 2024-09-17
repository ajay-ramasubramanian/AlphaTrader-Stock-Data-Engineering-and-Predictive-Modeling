import os
from spotipy import Spotify
import time
import spotipy
from dotenv import load_dotenv

from base_producer import SpotifyKafkaProducer
from utils import scope

# Load environment variables from .env file (if needed)
load_dotenv()
# clientID = os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

class ArtistAlbumsProducer(SpotifyKafkaProducer):
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


    def get_all_artist_albums(sp, artist_id):
        albums = []
        results = sp.artist_albums(artist_id, album_type='album,single,compilation', limit=50)
        albums.extend(results['items'])
        while results['next']:
            results = sp.next(results)
            albums.extend(results['items'])
        return albums


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

            for artist_id in artist_ids:
                albums = self.get_all_artist_albums(self.sp, artist_id)
                ## producer for artist_albums topic

                future = self.produce_artist_albums(albums)
                futures.append(future)

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
    artist_albums_producer = ArtistAlbumsProducer()
    artist_albums_producer.process_spotify_data()
