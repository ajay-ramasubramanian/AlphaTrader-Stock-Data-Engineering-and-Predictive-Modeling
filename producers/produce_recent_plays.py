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
    producer = SpotifyKafkaProducer()
    futures = []

    try:
        # after = None
        before = int(datetime.now().timestamp() * 1000)
        limit = 1
        track_count = 0
        max_tracks = 100

        while track_count < max_tracks:
            result = sp.current_user_recently_played(limit=limit, before=before)
            if not result['items']:
                break
            
            # Send to Kafka as soon as we have the data
            future = producer.produce_recent_plays(user_id, result)
            futures.append(future)

            played_at = datetime.strptime(result['items'][0]['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            # Update the 'after' parameter for the next request
            # after = int(played_at.timestamp() * 1000)
            before = int(played_at.timestamp() * 1000)
            track_count += 1
            print(f"Processed track {track_count}")

        print(f"Sent {track_count} tracks")

        # Wait for all messages to be sent
        for future in futures:
            try:
                kafka_future = future.result()
                record_metadata = kafka_future.get(timeout=10)
                print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            except Exception as e:
                print(f"Failed to send message: {e}")

    finally:
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