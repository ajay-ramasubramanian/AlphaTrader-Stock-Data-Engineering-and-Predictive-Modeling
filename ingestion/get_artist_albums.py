import sys, os
import site

# Extend the Python system path to include site packages and the parent directory of the current file
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary modules and functions
from data_checks.validate_expectations import validate_expectations
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Class to handle retrieval, processing, validation, and uploading of artist album data
class RetrieveArtistAlbums(MinioRetriever, MinioUploader):

    # Initialize the class with user, topic, raw and processed data paths
    def __init__(self, user, topic, raw, processed) -> None:

        # Set up retriever and uploader for MinIO using the provided user, topic, and paths
        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, topic, processed)
        self.processed = processed

        # Name of the Great Expectations suite to validate this data
        self.expectations_suite_name = 'artist_albums_suite'

        # Define the expected data types for the artist album DataFrame columns
        self.dtype_dict = {
            'album_name': str,
            'album_id': str,
            'album_type': str,  # allows NaNs
            'total_tracks': 'int64',
            'release_date': 'datetime64[ns]',
            'artist_name': str,
            'artist_id': str,
            'ingested_on': str
        }

    # Check if the 'ingested_on' column is valid: non-null, non-empty, and of type string
    def check_ingested_on(self, df):
        ingested_on_values = df['ingested_on'].tolist()
        
        # Check if all values are strings
        all_strings = all(isinstance(val, str) for val in ingested_on_values)
        # Ensure there are no empty strings
        no_empty_strings = all(val.strip() != '' for val in ingested_on_values if isinstance(val, str))
        # Ensure no null values
        no_null_values = df['ingested_on'].notnull().all()
        
        # Print detailed error messages if checks fail
        if not all_strings:
            print("Error: Not all values in 'ingested_on' are strings")
            non_string_indices = [i for i, val in enumerate(ingested_on_values) if not isinstance(val, str)]
            print(f"Non-string values found at indices: {non_string_indices}")
            print(f"Types of non-string values: {[type(ingested_on_values[i]) for i in non_string_indices]}")
        
        if not no_empty_strings:
            print("Error: Empty strings found in 'ingested_on'")
            empty_string_indices = [i for i, val in enumerate(ingested_on_values) if isinstance(val, str) and val.strip() == '']
            print(f"Empty strings found at indices: {empty_string_indices}")
        
        if not no_null_values:
            print("Error: Null values found in 'ingested_on'")
            null_indices = df.index[df['ingested_on'].isnull()].tolist()
            print(f"Null values found at indices: {null_indices}")
        
        # Return True only if all checks pass
        return all_strings and no_empty_strings and no_null_values

    # Main function to retrieve, process, validate, and upload artist album data
    def get_user_artist_albums(self):

        artists = []
        try:
            # Retrieve data from MinIO
            results = self.retriever.retrieve_object()
            # Process each result and extract relevant fields
            for result in results:
                artists.append({
                    'album_id': result['id'],
                    'album_name': result['name'],
                    'album_type': result['album_type'],
                    'total_tracks': result['total_tracks'],
                    'release_date': result['release_date'],
                    'artist_id': result['artists'][0]['id'],
                    'artist_name': result['artists'][0]['name']
                })

            # Convert the list of artist albums to a DataFrame
            df_artists = pd.DataFrame(artists)
            # Parse release_date and add the current ingestion timestamp
            df_artists['release_date'] = pd.to_datetime(df_artists['release_date'], format='mixed')
            df_artists['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Convert DataFrame columns to the expected data types
            df_artists = df_artists.astype(self.dtype_dict)
            # Drop duplicate records based on 'album_id' and 'artist_id'
            df_artists.drop_duplicates(['album_id', 'artist_id'], inplace=True)
            df_artists = df_artists.reset_index(drop=True)

            # Validate data using the Great Expectations suite
            validate_expectations(df_artists, self.expectations_suite_name)

            # Upload the processed and validated DataFrame back to MinIO
            self.uploader.upload_files(data=df_artists)
            print(f"Successfully uploaded to '{self.processed}' container!!")
        
        except Exception as e:
            print(f"Encountered an exception here!!: {e}")

# Function to run the artist album retrieval process
def run_get_user_artist_albums():
    ob = RetrieveArtistAlbums(os.getenv('USER_NAME'), \
                                TOPIC_CONFIG["artist_albums"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_user_artist_albums()

# Execute the function if the script is run directly
if __name__ == "__main__":
    run_get_user_artist_albums()