from base_producer import SpotifyKafkaProducer
import os
import time
from dotenv import load_dotenv

load_dotenv()

class TopTracksProducer(SpotifyKafkaProducer):
    def __init__(self):
        super().__init__()

    def process_spotify_data(self, user_id):
        """
        Processes Spotify data for the given user by retrieving their top tracks
        and sending this data to Kafka for downstream processing.

        Args:
            user_id (str): The Spotify user ID.
        """
        futures = []

        try:
            offset = 0
            time_range = "short_term"
            limit = 1
            print("Sending data to Kafka\n")
            while True:
                
                result = self.sp.current_user_top_tracks(time_range=time_range, limit=limit, offset=offset)
                time.sleep(0.2)
                if not result['items']:
                    break
                
                # Send to Kafka as soon as we have the data
                future = self.produce_top_songs(user_id, result)
                futures.append(future)
                
                offset += limit

            print("Sent all the data")

            # Wait for all messages to be sent
            for future in futures:
                try:
                    kafka_future = future.result()
                    record_metadata = kafka_future.get(timeout=10)
                    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                except Exception as e:
                    print(f"Failed to send message: {e}")

        finally:
            self.close()

def run_producer_top_tracks():
    top_tracks = TopTracksProducer()
    top_tracks.process_spotify_data(os.getenv('USER_NAME'))

if __name__ == "__main__":
    run_producer_top_tracks()