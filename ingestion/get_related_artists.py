import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG
from dotenv import load_dotenv

load_dotenv()

class RetrieveRelatedArtists():

    # Initialize the class with user and data storage configurations
    def __init__(self, user, topic, raw, processed) -> None:
        self.retriever = MinioRetriever(user, topic, raw)  # Retrieve raw data from MinIO
        self.uploader = MinioUploader(user, topic, processed)  # Upload processed data back to MinIO
        self.processed = processed  # Path for processed data storage
        self.expectations_suite_name = 'related_artists_suite'  # Great Expectations suite for data validation

        # Dictionary defining expected data types for the related artists DataFrame
        self.dtype_dict = {
            'artist_name': str,  # Name of the artist
            'artist_id': str,  # Unique identifier for the artist
            'artist_popularity': 'int64',  # Popularity score of the artist, allows NaNs
            'artist_followers': 'int64',  # Number of followers the artist has
            'genres': object,  # List of genres associated with the artist
            'ingested_on': str  # Timestamp for when the data was ingested
        }

    # Method to retrieve, process, validate, and upload related artists data
    def get_artist_related_artists(self):
        all_artists = []  # Initialize a list to store artist information
        results = self.retriever.retrieve_object()  # Retrieve raw data from MinIO

        try:
            # Process each artist's data from the retrieved results
            for result in results:
                all_artists.append({
                    'artist_id': result['id'],  # Extract artist ID
                    'artist_name': result['name'],  # Extract artist name
                    'artist_popularity': result['popularity'],  # Extract artist popularity
                    'artist_followers': result['followers'],  # Extract number of followers
                    'genres': result['genres']  # Extract genres
                })

            # Convert the list of artists into a DataFrame for further processing
            df_artists = pd.DataFrame(all_artists)
            df_artists['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")  # Add ingestion timestamp

            # Convert the DataFrame to the expected data types as defined in dtype_dict
            df_artists = df_artists.astype(self.dtype_dict)
            df_artists.drop_duplicates(['artist_id'], inplace=True)  # Remove duplicate artist entries
            df_artists = df_artists.reset_index(drop=True)  # Reset index after deduplication

            # Validate the DataFrame using Great Expectations for data quality checks
            validate_expectations(df_artists, self.expectations_suite_name)

            # Upload the validated data back to MinIO
            self.uploader.upload_files(data=df_artists)
            print(f"Successfully uploaded to '{self.processed}' container!!")  # Confirmation message

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")  # Error handling for exceptions encountered

# Function to initialize and run the related artists retrieval process
def run_get_artist_related_artists():
    ob = RetrieveRelatedArtists(os.getenv('USER_NAME'), \
                                TOPIC_CONFIG["related_artists"]["topic"], \
                                "raw", \
                                "processed")  # Create an instance of RetrieveRelatedArtists
    ob.get_artist_related_artists()  # Call the method to get related artists

if __name__ == "__main__":
    run_get_artist_related_artists()  # Execute the function if the script is run directly
