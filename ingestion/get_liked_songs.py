import sys,os
import site
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
import pandas as pd
import pytz
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG


class RetrieveLikedSongs(LoggingMixin):

    def __init__(self,user, topic,raw, processed) -> None:

        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user,topic, processed)
        self.processed = processed
        self.expectations_suite_name = 'liked_songs_suite'

        self.dtype_dict = {
            'like_id': 'int64',
            'artist_id': str,
            'album_id': str,
            'track_id': str,
            'added_at': object,
            'time_id': str,
            'ingested_on': str
        }
        
    def get_user_liked_songs(self):

        try:
            tracks = []
            results= self.retriever.retrieve_object()
            # self.log.info(f"Retrieved {len(results)} results from Minio")

            for count, result in enumerate(results):
                item=result["items"]
                track = item[0]['track']
                tracks.append({
                    'like_id': count,
                    'artist_id': track['artists'][0]['id'],
                    'album_id': track['album']['id'],
                    'track_id': track['id'],
                    'added_at': item[0]['added_at']
                })

            # Convert to DataFrame
            df_tracks= pd.DataFrame(tracks)
            df_tracks['added_at'] = pd.to_datetime(df_tracks['added_at']).dt.tz_convert(pytz.UTC)
            df_tracks['time_id'] = df_tracks['added_at'].apply(lambda val: val.strftime('%Y%m%d%H%M%S'))
            df_tracks['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")

            df_tracks = df_tracks.astype(self.dtype_dict)
            df_tracks.drop_duplicates(subset=['track_id'], inplace=True)
            df_tracks = df_tracks.reset_index(drop=True)

            # Run Great Expectations data quality checks
            validate_expectations(df_tracks, self.expectations_suite_name)

            self.uploader.upload_files(data=df_tracks)
            print(f"Successfully uploaded to '{self.processed}' container!!")
        
        except Exception as e:
            print(f"Encountered an exception here!!: {e}")


def run_retrieve_liked_songs(**context):
    # task_instance = context['task_instance']
    # task_instance.log.info("Starting retrieve_liked_songs task")
    ob = RetrieveLikedSongs("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_user_liked_songs()

    

if __name__ == "__main__":
    run_retrieve_liked_songs()