from base_producer import SpotifyKafkaProducer
import os
import time
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

# Load environment variables from .env file (if needed)
load_dotenv()

class SavedTracksProducer(SpotifyKafkaProducer):
    def __init__(self):
        super().__init__()

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
            limit = 1  # Limit for number of items to fetch per request (can be adjusted)
            print("Sending data to Kafka\n")
            while True:
                
                # Fetch the current user's saved tracks with pagination support
                result = self.sp.current_user_playlists(limit=limit, offset=offset)
                time.sleep(0.2)

                # Break the loop if no items are returned
                if not result['items']:
                    break

                # Send the data to Kafka as soon as it is retrieved
                future = self.produce_saved_playlists(user_id, result)
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



def run_producer_saved_playlist():
    saved_tracks = SavedTracksProducer()
    saved_tracks.process_spotify_data(os.getenv('USER_NAME'))

if __name__ == "__main__":
    # Start the data processing for a specific user
    run_producer_saved_playlist()
