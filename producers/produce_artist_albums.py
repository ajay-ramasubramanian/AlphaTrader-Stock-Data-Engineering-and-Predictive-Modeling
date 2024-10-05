import os
from spotipy import Spotify
import time
import spotipy
from dotenv import load_dotenv
from base_producer import SpotifyKafkaProducer
from utils import scope

# Load environment variables from .env file (if needed)
# load_dotenv()
# clientID = os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

load_dotenv()
class ArtistAlbumsProducer(SpotifyKafkaProducer):
    """
    A producer class for sending a artists related to artists followed by the user to a Kafka topic.
    """

    def __init__(self):
        """
        Initialize the SavedTracksProducer with a Spotify client and Kafka producer.
        """
        super().__init__()


    def get_artist_ids(self, user_id, artist_ids):
        self.process_spotify_data(user_id, artist_ids)


    def get_all_artist_albums(self, sp, artist_id):
        albums = []
        results = sp.artist_albums(artist_id, album_type='album,single,compilation', limit=1)
        albums.append(results['items'])
        while results['next']:
            results = sp.next(results)
            albums.append(results['items'])
        return albums


    def process_spotify_data(self, user_id, artist_ids=None):
        futures = []
        try:
            print("Sending data to Kafka")
            artist_ids = list(dict.fromkeys(artist_ids))
            print(len(artist_ids))
    
            for artist_id in artist_ids:
                print(f"Processing artist: {artist_id}")
                albums = []
                result = self.sp.artist_albums(artist_id, album_type='album,single,compilation', limit=10)
                time.sleep(0.2)
                while result['items'] and len(albums) < 10:
                    albums.extend(result['items'])
                    if len(albums) < 10 and result['next']:
                        result = self.sp.next(result)
                    else:
                        break

                for album in albums:
                    future = self.produce_artist_albums(user_id, album)
                    print(f"Sent record to Kafka: {album['name']}")
                    futures.append(future)

                print(f"Processed {len(albums)} albums for artist {artist_id}")

            print("Sent all the data")

            # Wait for all Kafka messages to be sent and handle their results
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
   
    # Start the data processing for a specific user
    artist_albums_producer = ArtistAlbumsProducer()
    artist_albums_producer.process_spotify_data(os.getenv('USER_NAME'))
