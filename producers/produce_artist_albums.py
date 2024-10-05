import os
from spotipy import Spotify
import time
import spotipy
from dotenv import load_dotenv
from base_producer import SpotifyKafkaProducer
from utils import scope

# Load environment variables from a .env file that contains sensitive credentials 
# such as the Spotify API client ID, client secret, and redirect URI. 
# These variables are required to authenticate the app with Spotify's API.
load_dotenv()

class ArtistAlbumsProducer(SpotifyKafkaProducer):
    """
    A producer class that inherits from SpotifyKafkaProducer.
    This class is responsible for retrieving albums related to artists followed by a user 
    and sending this data to a Kafka topic for further processing.
    """

    def __init__(self):
        """
        Initialize the ArtistAlbumsProducer class.
        Calls the parent class's constructor to set up the Spotify client 
        and Kafka producer components required for API calls and data streaming.
        """
        super().__init__()

    def get_artist_ids(self, user_id, artist_ids):
        """
        A wrapper method to kickstart the data processing for Spotify artist IDs.
        :param user_id: The Spotify user ID whose data is being processed.
        :param artist_ids: A list of artist IDs to retrieve albums for.
        """
        self.process_spotify_data(user_id, artist_ids)

    def get_all_artist_albums(self, sp, artist_id):
        """
        Fetches all albums, singles, and compilations for a specific artist using Spotify's API.
        This method handles pagination by repeatedly calling Spotify's API until all albums are fetched.

        :param sp: The Spotify client instance.
        :param artist_id: The Spotify ID of the artist to fetch albums for.
        :return: A list of all albums for the artist.
        """
        albums = []
        # Request albums from the Spotify API with the specified album types.
        results = sp.artist_albums(artist_id, album_type='album,single,compilation', limit=1)
        albums.append(results['items'])
        
        # Continue fetching additional pages of albums if they exist (pagination).
        while results['next']:
            results = sp.next(results)
            albums.append(results['items'])
        
        return albums

    def process_spotify_data(self, user_id, artist_ids=None):
        """
        Processes artist data, retrieves albums for each artist, and sends this data to Kafka.
        This method handles multiple artists in parallel and ensures that messages are sent
        asynchronously to the Kafka topic.

        :param user_id: The Spotify user ID associated with the artist data being processed.
        :param artist_ids: A list of artist IDs to process. If None, no data is processed.
        """
        futures = []
        try:
            # If no artist IDs are provided, print a message and return.
            if artist_ids is None:
                print("I shall return!!")  # Placeholder message in case of missing artist IDs.
                return
            
            print("Sending data to Kafka\n")

            # Ensure the artist IDs list is unique by removing duplicates.
            artist_ids = list(dict.fromkeys(artist_ids))
            print(f"Total unique artists to process: {len(artist_ids)}")
    
            # Loop through each artist ID and fetch up to 10 albums for each artist.
            for artist_id in artist_ids:
                print(f"Processing artist: {artist_id}")
                albums = []
                # Fetch the first batch of albums for the artist.
                result = self.sp.artist_albums(artist_id, album_type='album,single,compilation', limit=10)
                time.sleep(0.2)  # Slight delay to avoid API rate limits.
                
                # Continue fetching until either 10 albums are retrieved or there are no more results.
                while result['items'] and len(albums) < 10:
                    albums.extend(result['items'])
                    if len(albums) < 10 and result['next']:
                        result = self.sp.next(result)
                    else:
                        break

                # Send each album's data to the Kafka topic asynchronously.
                for album in albums:
                    future = self.produce_artist_albums(user_id, album)  # Send the album to Kafka.
                    print(f"Sent record to Kafka: {album['name']}")
                    futures.append(future)

                print(f"Processed {len(albums)} albums for artist {artist_id}")

            print("Sent all the data")

            # Wait for all Kafka messages to be sent and handle their results.
            for future in futures:
                try:
                    kafka_future = future.result()
                    record_metadata = kafka_future.get(timeout=10)
                    # Print Kafka metadata if the message was successfully sent.
                    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                except Exception as e:
                    # Log any failures that occurred while sending messages.
                    print(f"Failed to send message: {e}")

        finally:
            # Ensure the Kafka producer is properly closed once processing is complete.
            self.close()

if __name__ == "__main__":
    # Initialize the producer and start the process for a specific Spotify user.
    artist_albums_producer = ArtistAlbumsProducer()
    # Replace 'USER_NAME' with the actual username, pulling it from environment variables.
    artist_albums_producer.process_spotify_data(os.getenv('USER_NAME'))
