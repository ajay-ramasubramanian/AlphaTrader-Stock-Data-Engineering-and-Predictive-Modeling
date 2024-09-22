import sys,os
import site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG


class RetrieveLikedSongs():

    def __init__(self,user, topic,raw, processed) -> None:
        self.retriver = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, 'spotify_all_tracks', processed)
    def get_user_liked_songs(self):
        tracks = []
        results= self.retriver.retrieve_object()
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
        print(df_tracks.head(10))
        print(f"In ingestion: {len(df_tracks['track_id'])}")
        self.uploader.upload_files(data=df_tracks)
        print("Object uploaded")


def run_retrieve_liked_songs():
    ob = RetrieveLikedSongs("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_user_liked_songs()

    

if __name__ == "__main__":
    run_retrieve_liked_songs()