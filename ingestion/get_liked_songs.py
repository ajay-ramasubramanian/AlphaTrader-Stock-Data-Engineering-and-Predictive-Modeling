from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd
from utils import TOPIC_CONFIG

class RetrieveLikedSongs(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,container) -> None:
        MinioRetriever.__init__(self,user, topic)
        MinioUploader.__init__(self,container,user,topic)
    def get_liked_songs(self):
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


    

if __name__ == "__main__":
    ob = RetrieveLikedSongs("suhaas",TOPIC_CONFIG["liked_songs"]["topic"],"processed")
    ob.get_liked_songs()