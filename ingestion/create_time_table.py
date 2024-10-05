import sys, os
import site

# Extend the system path to include site-specific directories and the parent directory of the current file.
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary modules for data validation, retrieval, and file upload operations.
from data_checks.validate_expectations import validate_expectations
import pandas as pd
from transformations.utils import MinioRetriever, MinioUploader
from ingestion.utils import TOPIC_CONFIG

# Load environment variables from the .env file.
from dotenv import load_dotenv
load_dotenv()

class CreateTimeTable:
    """
    A class responsible for creating a time table based on the 'liked_songs' data.
    It retrieves the 'liked_songs' data from MinIO storage, processes the timestamps,
    and uploads the results back to MinIO after performing data validation.
    """

    TOPIC = "spotify-time-table"  # Kafka topic name for the time table data.

    def __init__(self, user, topic, processed, presentation) -> None:
        """
        Initialize the CreateTimeTable class with MinIO retrieval and upload mechanisms.

        Args:
            user (str): The user associated with the data retrieval and upload.
            topic (str): The Kafka topic for the 'liked_songs' data.
            processed (str): The name of the processed data layer.
            presentation (str): The name of the presentation data layer.
        """
        # Initialize MinIO retriever for the 'liked_songs' data.
        self.retriver = MinioRetriever(user, topic, processed, os.getenv('HOST'))
        # Initialize MinIO uploader for uploading the processed data.
        self.uploader = MinioUploader(user, self.TOPIC, presentation, os.getenv('HOST'))
        self.presentation = presentation

        # Set the name of the Great Expectations validation suite for this data.
        self.expectation_suite_name = "time_table_suite"

        # Define the expected data types for the time table.
        self.dtype_dict = {
            'date_id': str,
            'year': 'int64',
            'month': 'int64',
            'day': 'int64',
            'hour': 'int64',
            'minute': 'int64',
            'second': 'int64'
        }

    def create_time_table(self):
        """
        Creates the time table by extracting and processing unique timestamps from the 'liked_songs' data.
        Validates the result using Great Expectations and uploads it to MinIO.
        """
        try:
            print("Ingesting time table!!")
            # Retrieve 'liked_songs' data from MinIO.
            liked_songs = self.retriver.retrieve_object()
            liked_songs['added_at'] = pd.to_datetime(liked_songs['added_at'])  # Convert timestamps to datetime format.
            date_time = pd.Series(liked_songs['added_at'].unique())  # Extract unique timestamps.

            # Create a dictionary containing date components.
            date_dict = {
                'date_id': date_time.dt.strftime('%Y%m%d%H%M%S'),
                'year': date_time.dt.year,
                'month': date_time.dt.month,
                'day': date_time.dt.day,
                'hour': date_time.dt.hour,
                'minute': date_time.dt.minute,
                'second': date_time.dt.second
            }

            # Convert the dictionary into a DataFrame.
            date_time_df = pd.DataFrame(date_dict)
            date_time_df.astype(self.dtype_dict)  # Convert DataFrame to the correct data types.
            date_time_df = date_time_df.reset_index(drop=True)  # Reset index.

            # Run Great Expectations data quality checks.
            validate_expectations(date_time_df, self.expectation_suite_name)

            # Upload the processed time table to MinIO.
            self.uploader.upload_files(data=date_time_df)
            print(f"Successfully uploaded to '{self.presentation}' container!!")

        except Exception as e:
            # Handle any exceptions that occur during data processing or upload.
            print(f"Encountered an exception here!!: {e}")

def run_create_time_table():
    """
    Runs the process to create and upload the time table for a specific user.
    """
    ob = CreateTimeTable(os.getenv('USER_NAME'), \
                         TOPIC_CONFIG["liked_songs"]["topic"], \
                         "processed", \
                         "presentation")
    ob.create_time_table()

if __name__ == "__main__":
    # Entry point for running the time table creation process.
    run_create_time_table()
