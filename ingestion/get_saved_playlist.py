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

class RetrieveSavedPlaylist():

    def __init__(self,user, topic, raw, processed) -> None:
        
        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user,topic, processed)
        self.processed = processed
        self.expectations_suite_name = 'saved_playlist_suite'

        self.dtype_dict = {
            'playlist_name': str,
            'playlist_id': str,
            'playlist_uri': str,
            'owner_name': str,
            'owner_id': str,
            'is_public': bool,
            'is_collaborative': bool,
            'total_tracks': 'int64',
            'description': str,
            'ingested_on': str
        }

    def get_user_saved_playlist(self):

        try:
            playlists = []
            results = self.retriever.retrieve_object()
            for result in results:
                item = result["items"][0]
                playlists.append({
                    'playlist_name': item['name'],
                    'playlist_id': item['id'],
                    'playlist_uri': item['uri'],
                    'owner_name': item['owner']['display_name'],
                    'owner_id': item['owner']['id'],
                    'is_public': item['public'],
                    'is_collaborative': item['collaborative'],
                    'total_tracks': item['tracks']['total'],
                    'description': item['description']
                })

            # Convert to DataFrame
            df_playlist = pd.DataFrame(playlists)
            df_playlist['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")

            df_playlist = df_playlist.astype(self.dtype_dict)
            df_playlist.drop_duplicates(['playlist_id'], inplace=True)
            df_playlist = df_playlist.reset_index(drop=True)

            # Run Great Expectations data quality checks
            validate_expectations(df_playlist, self.expectations_suite_name)

            self.uploader.upload_files(data=df_playlist)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")
    

def run_retrieve_saved_playlist():
    ob = RetrieveSavedPlaylist(os.getenv('USER_NAME'), \
                            TOPIC_CONFIG["saved_playlists"]["topic"],\
                            "raw", \
                            "processed")
    ob.get_user_saved_playlist()


if __name__ == "__main__":
    run_retrieve_saved_playlist()