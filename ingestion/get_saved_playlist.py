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

class RetrieveSavedPlaylist():

    # Initialize the class with user and data storage configurations
    def __init__(self,user, topic, raw, processed) -> None:
        self.retriever = MinioRetriever(user, topic, raw)  # Retrieve raw playlist data from MinIO
        self.uploader = MinioUploader(user, topic, processed)  # Upload processed playlist data back to MinIO
        self.processed = processed  # Path for processed data storage
        self.expectations_suite_name = 'saved_playlist_suite'  # Great Expectations suite for data validation

        # Dictionary defining expected data types for the saved playlist DataFrame
        self.dtype_dict = {
            'playlist_name': str,  # Name of the playlist
            'playlist_id': str,  # Unique identifier for the playlist
            'playlist_uri': str,  # URI link for the playlist
            'owner_name': str,  # Name of the playlist owner
            'owner_id': str,  # Unique identifier for the owner
            'is_public': bool,  # Indicates if the playlist is public
            'is_collaborative': bool,  # Indicates if the playlist is collaborative
            'total_tracks': 'int64',  # Total number of tracks in the playlist
            'description': str,  # Description of the playlist
            'ingested_on': str  # Timestamp for when the data was ingested
        }

    # Method to retrieve, process, validate, and upload saved playlist data
    def get_user_saved_playlist(self):
        try:
            playlists = []  # Initialize a list to store playlist information
            results = self.retriever.retrieve_object()  # Retrieve raw data from MinIO

            # Process each playlist from the retrieved results
            for result in results:
                item = result["items"][0]  # Access the first item in the list
                playlists.append({
                    'playlist_name': item['name'],  # Extract playlist name
                    'playlist_id': item['id'],  # Extract playlist ID
                    'playlist_uri': item['uri'],  # Extract playlist URI
                    'owner_name': item['owner']['display_name'],  # Extract owner's display name
                    'owner_id': item['owner']['id'],  # Extract owner's ID
                    'is_public': item['public'],  # Check if the playlist is public
                    'is_collaborative': item['collaborative'],  # Check if the playlist is collaborative
                    'total_tracks': item['tracks']['total'],  # Get total number of tracks
                    'description': item['description']  # Extract description of the playlist
                })

            # Convert the list of playlists into a DataFrame for further processing
            df_playlist = pd.DataFrame(playlists)
            df_playlist['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")  # Add ingestion timestamp

            # Convert the DataFrame to the expected data types as defined in dtype_dict
            df_playlist = df_playlist.astype(self.dtype_dict)
            df_playlist.drop_duplicates(['playlist_id'], inplace=True)  # Remove duplicate playlist entries
            df_playlist = df_playlist.reset_index(drop=True)  # Reset index after deduplication

            # Validate the DataFrame using Great Expectations for data quality checks
            validate_expectations(df_playlist, self.expectations_suite_name)

            # Upload the validated data back to MinIO
            self.uploader.upload_files(data=df_playlist)
            print(f"Successfully uploaded to '{self.processed}' container!!")  # Confirmation message

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")  # Error handling for exceptions encountered

# Function to initialize and run the saved playlist retrieval process
def run_retrieve_saved_playlist():
    ob = RetrieveSavedPlaylist(os.getenv('USER_NAME'), \
                                TOPIC_CONFIG["saved_playlists"]["topic"], \
                                "raw", \
                                "processed")  # Create an instance of RetrieveSavedPlaylist
    ob.get_user_saved_playlist()  # Call the method to get saved playlists

if __name__ == "__main__":
    run_retrieve_saved_playlist()  # Execute the function if the script is run directly
