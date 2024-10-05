import sys, os
import site

# Extend the system path to include site-specific directories and the parent directory of the current file.
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary modules for data validation, retrieval, and file upload operations.
from data_checks.validate_expectations import validate_expectations
import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever, MinioUploader
from ingestion.utils import TOPIC_CONFIG

class RetrieveAllTracks:
    """
    A class responsible for retrieving all tracks from the 'liked_songs' data.
    It retrieves the raw data from MinIO storage, processes it to extract track information,
    and uploads the results back to MinIO after performing data validation.
    """

    TOPIC = 'spotify_all_tracks'  # Kafka topic name for the all tracks data.

    def __init__(self, user, topic, raw, processed) -> None:
        """
        Initialize the RetrieveAllTracks class with MinIO retrieval and upload mechanisms.

        Args:
            user (str): The user associated with the data retrieval and upload.
            topic (str): The Kafka topic for the 'liked_songs' data.
            raw (str): The name of the raw data layer.
            processed (str): The name of the processed data layer.
        """
        # Initialize MinIO retriever for raw data.
        self.retriver = MinioRetriever(user, topic, raw)
        # Initialize MinIO uploader for processed data.
        self.uploader = MinioUploader(user, self.TOPIC, processed)
        self.processed = processed

        # Set the name of the Great Expectations validation suite for this data.
        self.expectations_suite_name = 'all_tracks_suite'

        # Define the expected data types for the all tracks table.
        self.dtype_dict = {
            'track_id': str,
            'track_name': str,
            'duration_ms': 'int64',
            'track_popularity': 'int64',
            'track_uri': str,
            'album_name': str,
            'artist_name': str,
            'ingested_on': str
        }

    def get_all_tracks(self):
        """
        Retrieves and processes the track data from the 'liked_songs' data.
        Validates the result using Great Expectations and uploads it to MinIO.
        """
        tracks = []  # List to store track data.

        try:
            # Retrieve raw track data from MinIO.
            results = self.retriver.retrieve_object()

            # Iterate through the raw data and extract track information.
            for result in results:
                item = result["items"]
                track = item[0]['track']  # Extract the track details.
                tracks.append({
                    'track_id': track['id'],
                    'track_name': track['name'],
                    'duration_ms': track['duration_ms'],
                    'track_popularity': track['popularity'],
                    'track_uri': track['uri'],
                    'album_name': track['album']['name'],
                    'artist_name': track['artists'][0]['name']
                })

            # Convert the list of tracks into a DataFrame.
            df_tracks = pd.DataFrame(tracks)
            df_tracks['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")  # Add ingestion timestamp.

            # Convert the DataFrame to the correct data types.
            df_tracks = df_tracks.astype(self.dtype_dict)
            df_tracks.drop_duplicates(subset='track_id', inplace=True)  # Remove duplicate tracks.
            df_tracks = df_tracks.reset_index(drop=True)  # Reset index.

            # Run Great Expectations data quality checks.
            validate_expectations(df_tracks, self.expectations_suite_name)

            # Upload the processed track DataFrame to MinIO.
            self.uploader.upload_files(data=df_tracks)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            # Handle any exceptions that occur during data processing or upload.
            print(f"Encountered an exception here!!: {e}")

def run_retrieve_all_tracks():
    """
    Runs the process to retrieve and upload all tracks data for a specific user.
    """
    ob = RetrieveAllTracks(os.getenv('USER_NAME'), \
                           TOPIC_CONFIG["liked_songs"]["topic"], \
                           "raw", \
                           "processed")
    ob.get_all_tracks()

if __name__ == "__main__":
    # Entry point for running the all tracks retrieval process.
    run_retrieve_all_tracks()
