import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG


class RetrieveAllTracks():

    def __init__(self,user, topic, raw, processed) -> None:

        self.retriver = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, 'spotify_all_tracks', processed)
        self.processed = processed

        self.dtype_dict = {
            'track_id': str,
            'track_name': str,
            'duration_ms': 'int64',
            'track_popularity': 'int64',
            'track_uri': str,
            'album_name': str,
            'artist_name': str,
            'ingested_on': str
        }

    def get_all_tracks(self):

        tracks = []

        try:
            results = self.retriver.retrieve_object()
            for result in results:
                item=result["items"]
                track = item[0]['track']
                tracks.append({
                    'track_id': track['id'],
                    'track_name': track['name'],
                    'duration_ms': track['duration_ms'],
                    'track_popularity': track['popularity'],
                    'track_uri': track['uri'],
                    'album_name': track['album']['name'],
                    'artist_name': track['artists'][0]['name']
                })

            # Convert to DataFrame
            df_tracks= pd.DataFrame(tracks)
            df_tracks['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")

            df_tracks = df_tracks.astype(self.dtype_dict)
            df_tracks.drop_duplicates(subset='track_id', inplace=True)
            df_tracks = df_tracks.reset_index(drop=True)
            
            self.uploader.upload_files(data=df_tracks)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")


def run_retrieve_all_tracks():
    ob = RetrieveAllTracks("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_all_tracks()

    

if __name__ == "__main__":
    run_retrieve_all_tracks()