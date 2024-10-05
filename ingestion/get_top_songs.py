import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG
from dotenv import load_dotenv

load_dotenv()

class RetrieveTopSongs():

    # Initialize the class with user and topic configurations
    def __init__(self, user, topic, raw, processed) -> None:
        self.retriever = MinioRetriever(user, topic, raw)  # Set up the retriever for raw data
        self.uploader = MinioUploader(user, topic, processed)  # Set up the uploader for processed data
        self.processed = processed  # Path for storing processed data
        self.expectations_suite_name = 'top_songs_suite'  # Suite name for data quality checks

    # Method to retrieve and process the user's top songs
    def get_user_top_songs(self):
        
        try:
            tracks = []  # Initialize a list to hold song data
            results = self.retriever.retrieve_object()  # Retrieve raw data from MinIO
            
            # Process each track's information from the retrieved results
            for result in results:
                item = result["items"][0]  # Access the first item in the result
                tracks.append({
                    'track_name': item['name'],  # Extract track name
                    'track_id': item['id'],  # Extract track ID
                    'track_uri': item['uri'],  # Extract track URI
                    'artist_name': item['artists'][0]['name'],  # Extract first artist's name
                    'artist_id': item['artists'][0]['id'],  # Extract first artist's ID
                    'album_name': item['album']['name'],  # Extract album name
                    'album_id': item['album']['id'],  # Extract album ID
                    'album_release_date': item['album']['release_date'],  # Extract album release date
                    'duration_ms': item['duration_ms'],  # Extract duration of the track in milliseconds
                    'popularity': item['popularity'],  # Extract track popularity score
                    'explicit': item['explicit'],  # Check if the track is explicit
                    'external_url': item['external_urls']['spotify'],  # Get external Spotify URL
                })

            # Convert the list of tracks into a DataFrame for further processing
            df_songs = pd.DataFrame(tracks)
            df_songs['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")  # Add a timestamp for ingestion
            
            df_songs.drop_duplicates(['track_id'], inplace=True)  # Remove duplicate entries based on track ID
            df_songs = df_songs.reset_index(drop=True)  # Reset index after deduplication

            # Validate the DataFrame using Great Expectations for data quality checks
            validate_expectations(df_songs, self.expectations_suite_name)

            # Upload the validated data back to MinIO
            self.uploader.upload_files(data=df_songs)
            print(f"Successfully uploaded to '{self.processed}' container!!")  # Confirmation message

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")  # Handle any exceptions encountered

# Function to initialize and run the retrieval process for top songs
def run_retrieve_top_songs():
    obj = RetrieveTopSongs(os.getenv('USER_NAME'), \
                    TOPIC_CONFIG["top_songs"]["topic"], \
                    "raw", \
                    "processed")  # Create an instance of RetrieveTopSongs
    obj.get_user_top_songs()  # Call the method to retrieve top songs

if __name__ == "__main__":
    run_retrieve_top_songs()  # Execute the function if the script is run directly

