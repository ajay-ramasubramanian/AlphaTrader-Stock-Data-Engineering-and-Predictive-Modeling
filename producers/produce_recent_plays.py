import os
from datetime import datetime

import pandas as pd
import spotipy
from datetime import datetime
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth

from base_producer import SpotifyKafkaProducer
from utils import scope

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))


def process_spotify_data(user_id):
    """
    Processes Spotify data for the given user by retrieving their recently played tracks 
    and sending this data to Kafka for downstream processing.

    Args:
        user_id (str): The Spotify user ID.
    """
    # Initialize Kafka producer
    producer = SpotifyKafkaProducer()
    futures = []  # List to keep track of future objects for asynchronous Kafka sends

    try:
        after = None  # Cursor for pagination in Spotify API based on timestamp
        limit = 1  # Limit for number of items to fetch per request (can be adjusted)

        while True:
            # Fetch the current user's recently played tracks with pagination support
            result = sp.current_user_recently_played(limit=limit, after=after)

            # Break the loop if no items are returned
            if not result['items']:
                break

            # Send the data to Kafka as soon as it is retrieved
            future = producer.produce_recent_plays(user_id, result)
            futures.append(future)

            # Convert the 'played_at' timestamp to a format suitable for the 'after' parameter
            played_at = datetime.strptime(result['item']['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            after = int(played_at.timestamp() * 1000)  # Convert to milliseconds

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
    process_spotify_data('suhaas')


        # limit = 50  # Maximum allowed by Spotify API
        # track_count = 0
        # max_tracks = 100

        # while track_count < max_tracks:
        #     result = sp.current_user_recently_played(limit=limit, before=before)
            
        #     if not result['items']:
        #         print("No more tracks to process")
        #         break
            
        #     # for item in result['items']:
        #         # Send to Kafka as soon as we have the data
        #     future = producer.produce_recent_plays(user_id, result)
        #     futures.append(future)

        #     played_at = datetime.strptime(result["items"][0]['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
        #     before = int(played_at.timestamp() * 1000)
            
        #     track_count += 1
        #     print(f"Processed track {track_count}")

        #     if track_count >= max_tracks:
        #         break

        #     # Add a small delay to avoid hitting rate limits
        #     # time.sleep(1)

        # print(f"Sent {track_count} tracks")
