import sys,os
import site
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
import pandas as pd
import pytz
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG

from dotenv import load_dotenv

load_dotenv()
class RetrieveLikedSongs(LoggingMixin):

    # Initialize the class with user information and MinIO retriever/uploader configurations
    def __init__(self, user, topic, raw, processed) -> None:
        self.retriever = MinioRetriever(user, topic, raw)  # Retrieve data from MinIO storage
        self.uploader = MinioUploader(user, topic, processed)  # Upload processed data back to MinIO
        self.processed = processed  # Path to store the processed data
        self.expectations_suite_name = 'liked_songs_suite'  # Great Expectations suite for data validation

        # Dictionary to define the expected data types for each field in the liked songs DataFrame
        self.dtype_dict = {
            'like_id': 'int64',  # Unique identifier for each liked song entry
            'artist_id': str,  # Artist ID related to the track
            'album_id': str,  # Album ID for the track
            'track_id': str,  # Unique identifier for the track
            'added_at': object,  # When the track was liked by the user
            'time_id': str,  # Timestamp formatted as a string
            'ingested_on': str  # Ingestion timestamp
        }

    # Method to retrieve, process, validate, and upload liked songs data
    def get_user_liked_songs(self):
        try:
            tracks = []  # Initialize list to store track information
            results = self.retriever.retrieve_object()  # Retrieve raw data from MinIO

            # Process each track from the raw data
            for count, result in enumerate(results):
                item = result["items"]
                track = item[0]['track']
                tracks.append({
                    'like_id': count,  # Assign a unique ID to each liked song
                    'artist_id': track['artists'][0]['id'],  # Get the artist ID
                    'album_id': track['album']['id'],  # Get the album ID
                    'track_id': track['id'],  # Get the track ID
                    'added_at': item[0]['added_at']  # When the song was liked
                })

            # Convert the list of liked songs into a DataFrame for further processing
            df_tracks = pd.DataFrame(tracks)
            df_tracks['added_at'] = pd.to_datetime(df_tracks['added_at']).dt.tz_convert(pytz.UTC)  # Convert added_at to UTC timezone
            df_tracks['time_id'] = df_tracks['added_at'].apply(lambda val: val.strftime('%Y%m%d%H%M%S'))  # Format the timestamp
            df_tracks['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")  # Add ingestion timestamp

            # Convert the DataFrame to the expected data types
            df_tracks = df_tracks.astype(self.dtype_dict)
            # Remove duplicate track entries based on 'track_id' and 'time_id'
            df_tracks.drop_duplicates(subset=['track_id'], inplace=True)
            df_tracks.drop_duplicates(subset=['time_id'], inplace=True)
            df_tracks = df_tracks.reset_index(drop=True)  # Reset index after deduplication

            # Validate the data using Great Expectations suite
            validate_expectations(df_tracks, self.expectations_suite_name)

            # Upload the validated data to MinIO
            self.uploader.upload_files(data=df_tracks)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")

# Function to initialize and run the liked songs retrieval process
def run_retrieve_liked_songs():
    ob = RetrieveLikedSongs(os.getenv('USER_NAME'), \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_user_liked_songs()

if __name__ == "__main__":
    run_retrieve_liked_songs()
