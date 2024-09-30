import sys, os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
import pandas as pd
from transformations.utils import MinioRetriever, MinioUploader
from ingestion.utils import TOPIC_CONFIG
from dotenv import load_dotenv

load_dotenv()

class CreateTimeTable():

    TOPIC = "spotify-time-table"

    def __init__(self, user, topic, processed, presentation) -> None:
        self.retriver = MinioRetriever(user, topic, processed, os.getenv('HOST'))
        self.uploader = MinioUploader(user, self.TOPIC, presentation, os.getenv('HOST'))
        self.presentation = presentation

        self.expectation_suite_name = "time_table_suite"

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
        try:
            print("Ingesting time table!!")
            liked_songs = self.retriver.retrieve_object()
            liked_songs['added_at'] = pd.to_datetime(liked_songs['added_at'])
            date_time = pd.Series(liked_songs['added_at'].unique())

            date_dict = {
                'date_id': date_time.dt.strftime('%Y%m%d%H%M%S'),
                'year': date_time.dt.year,
                'month': date_time.dt.month,
                'day': date_time.dt.day,
                'hour': date_time.dt.hour,
                'minute': date_time.dt.minute,
                'second': date_time.dt.second
            }

            date_time_df = pd.DataFrame(date_dict)
            date_time_df.astype(self.dtype_dict)
            date_time_df = date_time_df.reset_index(drop=True)

            # Run Great Expectations data quality checks
            validate_expectations(date_time_df, self.expectation_suite_name)

            # Upload the processed dataframe
            self.uploader.upload_files(data=date_time_df)
            print(f"Successfully uploaded to '{self.presentation}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")


def run_create_time_table():
    ob = CreateTimeTable(
        "suhaas", 
        TOPIC_CONFIG["liked_songs"]["topic"], 
        "processed", 
        "presentation"
    )
    ob.create_time_table()

if __name__ == "__main__":
    run_create_time_table()
