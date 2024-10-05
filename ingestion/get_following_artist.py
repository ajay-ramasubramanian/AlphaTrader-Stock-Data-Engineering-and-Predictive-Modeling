import sys ,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
from datetime import datetime
import pandas as pd
from ingestion.utils import TOPIC_CONFIG
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from dotenv import load_dotenv

load_dotenv()
class RetrieveFollowingArtists():

    # Initialize the class with user, topic, raw and processed data paths
    def __init__(self, user, topic, raw, processed) -> None:

        # Set up retriever and uploader for MinIO
        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, topic, processed)
        self.processed = processed

        # Name of the Great Expectations suite for this data
        self.expectations_suite_name = 'following_artists_suite'

        # Define the expected data types for the followed artists DataFrame columns
        self.dtype_dict = {
            'follow_id': 'int64',
            'artist_id': str,
            'ingested_on': str
        }

    # Function to retrieve, process, validate, and upload followed artist data
    def get_user_followed_artists(self):

        try:
            artists = []
            # Retrieve data from MinIO
            results = self.retriever.retrieve_object()
            # Process each result to extract artist ID and create a follow_id
            for count, result in enumerate(results):
                for item in result['artists']['items']:
                    artists.append({
                        'follow_id': count+1,
                        'artist_id': item['id']
                    })
            
            # Convert the list of followed artists to a DataFrame
            df_following_artist = pd.DataFrame(artists)
            # Add the current ingestion timestamp
            df_following_artist['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Convert DataFrame columns to the expected data types
            df_following_artist = df_following_artist.astype(self.dtype_dict)
            # Drop duplicate records based on 'artist_id'
            df_following_artist.drop_duplicates(subset=['artist_id'], inplace=True)
            df_following_artist = df_following_artist.reset_index(drop=True)
            
            # Validate data using the Great Expectations suite
            validate_expectations(df_following_artist, self.expectations_suite_name)

            # Upload the processed and validated DataFrame back to MinIO
            self.uploader.upload_files(data=df_following_artist)
            print(f"Successfully uploaded to '{self.processed}' container!!")
        
        except Exception as e:
            print(f"Encountered an exception here!!: {e}")

# Function to run the followed artist retrieval process
def run_retrieve_following_artists():
    ob = RetrieveFollowingArtists(os.getenv('USER_NAME'), \
                                TOPIC_CONFIG["following_artists"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_user_followed_artists()

# Execute the function if the script is run directly
if __name__ == "__main__":
    run_retrieve_following_artists()