import sys
import site
sys.path.extend(site.getsitepackages())
import pandas as pd
from .retrieve_objects import MinioRetriever,MinioUploader
from .utils import TOPIC_CONFIG

class RetrieveLikedSongs(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,raw, processed) -> None:
        MinioRetriever.__init__(self,user, topic, raw)
        MinioUploader.__init__(self,user,topic, processed)
    def get_user_liked_songs(self):
        tracks = []
        results= MinioRetriever.retrieve_object(self)
        for result in results:
            item=result["items"]
            track = item[0]['track']
            tracks.append({
                'name': track['name'],
                'artist': track['artists'][0]['name'],
                'album': track['album']['name'],
                'release_date': track['album']['release_date'],
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'id': track['id'],
                'uri': track['uri'],
                'added_at': item[0]['added_at']
            })
        # Convert to DataFrame
        df_tracks= pd.DataFrame(tracks)
        MinioUploader.upload_files(self,data=df_tracks)
        print("Object uploaded")


def run_retrieve_liked_songs():
    ob = RetrieveLikedSongs("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_user_liked_songs()

    

if __name__ == "__main__":
    run_retrieve_liked_songs()