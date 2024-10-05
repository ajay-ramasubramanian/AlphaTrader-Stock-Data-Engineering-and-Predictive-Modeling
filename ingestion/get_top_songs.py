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
class RetrieveTopSongs():

    def __init__(self,user, topic, raw, processed) -> None:

        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user,topic, processed)
        self.processed = processed
        self.expectations_suite_name = 'top_songs_suite'


    def get_user_top_songs(self):
        
        try:
            tracks = []
            results = self.retriever.retrieve_object()
            for result in results:
                item = result["items"][0]
                tracks.append({
                    'track_name': item['name'],
                    'track_id': item['id'],
                    'track_uri': item['uri'],
                    'artist_name': item['artists'][0]['name'],
                    'artist_id': item['artists'][0]['id'],
                    'album_name': item['album']['name'],
                    'album_id': item['album']['id'],
                    'album_release_date': item['album']['release_date'],
                    'duration_ms': item['duration_ms'],
                    'popularity': item['popularity'],
                    'explicit': item['explicit'],
                    'external_url': item['external_urls']['spotify'],
                })

            # Convert to DataFrame
            df_songs = pd.DataFrame(tracks)
            df_songs['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S") 
            
            df_songs.drop_duplicates(['track_id'], inplace=True)
            df_songs = df_songs.reset_index(drop=True)

            # Run Great Expectations data quality checks
            validate_expectations(df_songs, self.expectations_suite_name)

            self.uploader.upload_files(data=df_songs)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")


def run_retrieve_top_songs():
    obj= RetrieveTopSongs(os.getenv('USER_NAME'), \
                    TOPIC_CONFIG["top_songs"]["topic"], \
                    "raw", \
                    "processed")

    obj.get_user_top_songs()


if __name__ == "__main__":
    run_retrieve_top_songs()
