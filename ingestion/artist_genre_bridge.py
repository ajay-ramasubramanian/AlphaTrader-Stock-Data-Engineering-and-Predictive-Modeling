import sys, os
import site

# Extend the system path to include site-specific directories and the parent directory of the current file.
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary modules for data validation, retrieval, and file upload operations.
from data_checks.validate_expectations import validate_expectations
from transformations.utils import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

# Load environment variables from the .env file.
from dotenv import load_dotenv
load_dotenv()

class ArtistGenreBridge:
    """
    A class responsible for creating a bridge table between artists and genres.
    It retrieves data from MinIO storage, processes the data to form a bridge table,
    and uploads the results back to MinIO after performing data validation.
    """

    TOPIC = 'spotify_artist_genre_table'  # Kafka topic name for the artist-genre bridge data.

    def __init__(self, user, artist_table, genre_table, processed, presentation) -> None:
        """
        Initialize the ArtistGenreBridge class with MinIO retrieval and upload mechanisms.

        Args:
            user (str): The user associated with the data retrieval and upload.
            artist_table (str): The name of the artist table to retrieve.
            genre_table (str): The name of the genre table to retrieve.
            processed (str): The name of the processed data layer.
            presentation (str): The name of the presentation data layer.
        """
        # Initialize MinIO retrievers for artist and genre data.
        self.retrieve_artists = MinioRetriever(user, artist_table, processed, os.getenv('HOST'))
        self.retrieve_genres = MinioRetriever(user, genre_table, processed, os.getenv('HOST'))
        # Initialize MinIO uploader for uploading the processed data.
        self.uploader = MinioUploader(user, self.TOPIC, presentation, os.getenv('HOST'))
        self.presentation=presentation
        # Set the name of the Great Expectations validation suite for this data.
        self.expectations_suite_name = 'artist_genre_bridge_suite'

        # Define the expected data types for the artist-genre bridge table.
        self.dtype_dict = {
            'artist_id': str,
            'genre_id': 'int64'
        }

    def create_artist_genre_bridge(self):
        """
        Creates the artist-genre bridge table by merging artist and genre data.
        Validates the result using Great Expectations and uploads it to MinIO.
        """
        # Retrieve artist and genre data from MinIO.
        artists_df = self.retrieve_artists.retrieve_object()
        genres_df = self.retrieve_genres.retrieve_object()

        try:
            # Initialize an empty list to hold the bridge data.
            bridge = []

            # Iterate over each artist and their associated genres.
            for _, row in artists_df.iterrows():
                for genre in row['genres']:
                    bridge.append({'artist_id': row['artist_id'], 'genre': genre})

            # Convert the bridge data into a DataFrame.
            artists_genre_df = pd.DataFrame(bridge)

            # Merge the artist-genre data with the genres DataFrame.
            bridge_df = artists_genre_df.merge(genres_df, how="inner", on="genre")
            bridge_df = bridge_df[['artist_id', 'genre_id']]  # Select only the necessary columns.
            bridge_df.drop_duplicates(inplace=True)  # Remove duplicate rows.
            bridge_df = bridge_df.dropna()

            # Convert the DataFrame to the correct data types and reset the index.
            bridge_df.astype(self.dtype_dict)
            # bridge_df['genre_id'].astype(int)
            bridge_df.reset_index(drop=True, inplace=True)

            # Run Great Expectations data quality checks on the DataFrame.
            validate_expectations(bridge_df, self.expectations_suite_name)

            # Upload the processed artist-genre bridge DataFrame to MinIO.
            self.uploader.upload_files(data=bridge_df)
            print(f"Successfully uploaded to '{self.presentation}' container!!")

        except ValueError as e:
            # Handle value errors that may occur during data processing.
            print(f"Encountered a value error here!!: {e}")

def run_get_artist_genre_bridge():
    """
    Runs the process to create and upload the artist-genre bridge table for a specific user.
    """
    ob = ArtistGenreBridge(os.getenv('USER_NAME'), \
                           TOPIC_CONFIG["related_artists"]["topic"], \
                           "spotify_genres_table", \
                           "processed", \
                           "presentation")
    ob.create_artist_genre_bridge()

if __name__ == "__main__":
    # Entry point for running the artist-genre bridge creation process.
    run_get_artist_genre_bridge()
