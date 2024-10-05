import sys, os
import site

# Extend the system path to include site-specific directories and the parent directory of the current file.
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary modules for data validation, retrieval, and file upload operations.
from data_checks.validate_expectations import validate_expectations
from ingestion.retrieve_objects import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

# Load environment variables from the .env file.
from dotenv import load_dotenv
load_dotenv()

class CreateGenresTable:
    """
    A class responsible for creating the genres table.
    It retrieves genre data from MinIO storage, processes the data to create a genre table,
    and uploads the results back to MinIO after performing data validation.
    """

    TOPIC = 'spotify_genres_table'  # Kafka topic name for the genres data.

    def __init__(self, user, topic, raw, processed) -> None:
        """
        Initialize the CreateGenresTable class with MinIO retrieval and upload mechanisms.

        Args:
            user (str): The user associated with the data retrieval and upload.
            topic (str): The Kafka topic for the genre data.
            raw (str): The raw data layer name.
            processed (str): The processed data layer name.
        """
        # Initialize MinIO retriever for raw genre data.
        self.retriver = MinioRetriever(user, topic, raw)
        # Initialize MinIO uploader for processed data.
        self.uploader = MinioUploader(user, self.TOPIC, processed)
        self.processed = processed

        # Define the expected data types for the genre table.
        self.dtype_dict = {
            'genre_id': 'int64',
            'genre': str
        }

        # Set the name of the Great Expectations validation suite for this data.
        self.expectations_suite_name = 'genre_table_suite'

    def create_genre_table(self):
        """
        Creates the genre table by extracting unique genres from the raw data.
        Validates the result using Great Expectations and uploads it to MinIO.
        """
        try:
            unique_genres = set()  # Set to store unique genres.

            # Retrieve raw genre data from MinIO.
            results = self.retriver.retrieve_object()

            # Extract genres from the raw data.
            for result in results:
                genres = result['genres']
                for genre in genres:
                    unique_genres.add(genre)

            # Create a DataFrame from the unique genres.
            df_dict = dict(zip(range(1, len(unique_genres) + 1), unique_genres))
            df = pd.DataFrame(df_dict.items(), columns=['genre_id', 'genre'])

            # Convert the DataFrame to the correct data types and reset the index.
            df = df.astype(self.dtype_dict)
            df = df.reset_index(drop=True)

            # Run Great Expectations data quality checks on the DataFrame.
            validate_expectations(df, self.expectations_suite_name)

            # Upload the processed genre DataFrame to MinIO.
            self.uploader.upload_files(data=df)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except ValueError as e:
            # Handle value errors that may occur during data processing.
            print(f"Encountered a value error here!!: {e}")

def run_get_genre_table():
    """
    Runs the process to create and upload the genres table for a specific user.
    """
    ob = CreateGenresTable(os.getenv('USER_NAME'), \
                           TOPIC_CONFIG["related_artists"]["topic"], \
                           "raw", \
                           "processed")
    ob.create_genre_table()

if __name__ == "__main__":
    # Entry point for running the genre table creation process.
    run_get_genre_table()
