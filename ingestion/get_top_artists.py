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
class RetrieveTopArtists():

    # Initialize the class with user and topic configurations
    def __init__(self, user, topic, raw, processed) -> None:
        self.retriever = MinioRetriever(user, topic, raw)  # Set up the retriever for raw data
        self.uploader = MinioUploader(user, topic, processed)  # Set up the uploader for processed data
        self.processed = processed  # Path for storing processed data
        self.expectations_suite_name = 'top_artists_suite'  # Suite name for data quality checks

        # Define the expected data types for the DataFrame that will hold top artist information
        self.dtype_dict = {
            'artist_name': str,  # Name of the artist
            'artist_id': str,  # Unique identifier for the artist
            'artist_uri': str,  # URI link for the artist
            'popularity': 'int64',  # Popularity score of the artist
            'followers': 'int64',  # Number of followers the artist has
            'image_url': str,  # URL of the artist's image
            'spotify_url': str,  # External Spotify URL for the artist
            'ingested_on': str  # Timestamp for when the data was ingested
        }

    # Method to retrieve and process the user's top artists
    def get_user_top_artists(self):
        
        try:
            artists = []  # Initialize a list to hold artist data
            results = self.retriever.retrieve_object()  # Retrieve raw data from MinIO
            
            # Process each artist's information from the retrieved results
            for result in results:
                item = result["items"][0]  # Access the first item in the result
                artists.append({
                    'artist_id': item['id'],  # Extract artist ID
                    'artist_name': item['name'],  # Extract artist name
                    'artist_uri': item['uri'],  # Extract artist URI
                    'popularity': item['popularity'],  # Extract artist popularity
                    'followers': item['followers']['total'],  # Extract total number of followers
                    'genres': ', '.join(item['genres']),  # Combine genres into a single string
                    'image_url': item['images'][0]['url'] if item['images'] else None,  # Get image URL, if available
                    'spotify_url': item['external_urls']['spotify'],  # Get external Spotify URL
                })

            # Convert the list of artists into a DataFrame for further processing
            df_artists = pd.DataFrame(artists)
            df_artists['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")  # Add a timestamp for ingestion
            
            # Convert the DataFrame to the expected data types as defined in dtype_dict
            df_artists = df_artists.astype(self.dtype_dict)
            df_artists.drop_duplicates(['artist_id'], inplace=True)  # Remove duplicate entries based on artist ID
            df_artists = df_artists.reset_index(drop=True)  # Reset index after deduplication

            # Validate the DataFrame using Great Expectations for data quality checks
            validate_expectations(df_artists, self.expectations_suite_name)

            # Upload the validated data back to MinIO
            self.uploader.upload_files(data=df_artists)
            print(f"Successfully uploaded to '{self.processed}' container!!")  # Confirmation message

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")  # Handle any exceptions encountered

# Function to initialize and run the retrieval process for top artists
def run_retrieve_top_artists():
    ob = RetrieveTopArtists(os.getenv('USER_NAME'), \
                            TOPIC_CONFIG["top_artists"]["topic"], \
                            "raw", \
                            "processed")  # Create an instance of RetrieveTopArtists
    ob.get_user_top_artists()  # Call the method to retrieve top artists

if __name__ == "__main__":
    run_retrieve_top_artists()  # Execute the function if the script is run directly



