import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        
        # date_dict = dict()
        try: 
            print("Ingesting time table!!")
            liked_songs = self.retriver.retrieve_object()
            liked_songs['added_at'] = pd.to_datetime(liked_songs['added_at'])
            date_time = pd.Series(liked_songs['added_at'].unique())

            # date_dict['date_id'] = [dt.strftime('%Y%m%d%H%M%S') for dt in list(date_time)]
            # date_dict['year'] = list(date_time.dt.year)
            # date_dict['month'] = list(date_time.dt.month)
            # date_dict['day'] = list(date_time.dt.day)
            # date_dict['hour'] = list(date_time.dt.hour)
            # date_dict['minute'] = list(date_time.dt.minute)
            # date_dict['second'] = list(date_time.dt.second)
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

            print(date_time_df.dtypes)
            print(date_time_df['year'])

            self.uploader.upload_files(data=date_time_df)
            print(f"Successfully uploaded to '{self.presentation}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")

def run_create_time_table():
    ob = CreateTimeTable("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "processed", \
                            "presentation")
    ob.create_time_table()

    
if __name__ == "__main__":
    run_create_time_table()