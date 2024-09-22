import sys,os
import site
from datetime import datetime
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG

class RetrieveLikedSongs(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,raw, processed) -> None:
        MinioRetriever.__init__(self,user, topic, raw)
        MinioUploader.__init__(self,user,topic, processed)
    def get_user_liked_songs(self):
        tracks = []
        results= MinioRetriever.retrieve_object(self)
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

            # print(type(item[0]['added_at'].strftime('%Y%m%d%H%M%S')))
        # Convert to DataFrame
        df_tracks= pd.DataFrame(tracks)
        df_tracks['added_at'] = pd.to_datetime(df_tracks['added_at']) # data type is TIMESTAMP
        df_tracks['time_id'] = df_tracks['added_at'].apply(lambda val: val.strftime('%Y%m%d%H%M%S'))
        print(type(list(df_tracks['added_at'])[0]))
        df_tracks['ingested_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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