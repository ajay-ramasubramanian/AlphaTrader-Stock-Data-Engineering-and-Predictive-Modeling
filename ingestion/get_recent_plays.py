import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
from datetime import datetime
import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG
from dotenv import load_dotenv

load_dotenv()
class RetrieveRecentPlays():

    # Initialize the class with user information and MinIO retriever/uploader configurations
    def __init__(self, user, topic, raw, processed) -> None:
        self.retriever = MinioRetriever(user, topic, raw)  # Retrieve data from MinIO storage
        self.uploader = MinioUploader(user, topic, processed)  # Upload processed data back to MinIO
        self.processed = processed  # Path to store the processed data
        self.expectations_suite_name = 'recent_plays_suite'  # Great Expectations suite for data validation

        # Dictionary to define the expected data types for each field in the recent plays DataFrame
        self.dtype_dict = {
            'recents_id': 'int64',  # Unique identifier for each recent play entry
            'track_name': str,  # Name of the track
            'track_id': str,  # Unique identifier for the track
            'track_uri': str,  # URI link for the track
            'artist_name': str,  # Name of the artist
            'artist_id': str,  # Unique identifier for the artist
            'album_name': str,  # Name of the album
            'album_id': str,  # Unique identifier for the album
            'played_at': object,  # When the track was played by the user
            'duration_ms': 'int64',  # Duration of the track in milliseconds
            'popularity': 'int64',  # Popularity score of the track
            'ingested_on': str  # Ingestion timestamp
        }

    # Method to retrieve, process, validate, and upload recent plays data
    def get_user_recent_plays(self):
        try:
            tracks = []  # Initialize list to store track information
            results = self.retriever.retrieve_object()  # Retrieve raw data from MinIO

            # Process each recent play from the raw data
            for result in results:
                for count, item in enumerate(result["items"]):
                    track = item['track']
                    tracks.append({
                        'recents_id': count+1,  # Assign a unique ID to each recent play
                        'track_id': track['id'],  # Get the track ID
                        'track_name': track['name'],  # Get the track name
                        'track_uri': track['uri'],  # Get the track URI
                        'artist_name': track['artists'][0]['name'],  # Get the artist name
                        'artist_id': track['artists'][0]['id'],  # Get the artist ID
                        'album_name': track['album']['name'],  # Get the album name
                        'album_id': track['album']['id'],  # Get the album ID
                        'played_at': item['played_at'],  # Get the timestamp when the track was played
                        'duration_ms': track['duration_ms'],  # Track duration in milliseconds
                        'popularity': track['popularity']  # Popularity score of the track
                    })

            # Convert the list of recent plays into a DataFrame for further processing
            df_recent_plays = pd.DataFrame(tracks)
            df_recent_plays['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")  # Add ingestion timestamp

            # Convert the DataFrame to the expected data types
            df_recent_plays = df_recent_plays.astype(self.dtype_dict)
            # Remove duplicate play entries based on 'played_at'
            df_recent_plays.drop_duplicates(['played_at'], inplace=True)
            df_recent_plays = df_recent_plays.reset_index(drop=True)  # Reset index after deduplication

            # Validate the data using Great Expectations suite
            validate_expectations(df_recent_plays, self.expectations_suite_name)

            # Upload the validated data to MinIO
            self.uploader.upload_files(data=df_recent_plays)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")

# Function to initialize and run the recent plays retrieval process
def run_retrieve_recent_plays():
    ob = RetrieveRecentPlays(os.getenv('USER_NAME'), \
                            TOPIC_CONFIG["recent_plays"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_user_recent_plays()

if __name__ == "__main__":
    run_retrieve_recent_plays()
