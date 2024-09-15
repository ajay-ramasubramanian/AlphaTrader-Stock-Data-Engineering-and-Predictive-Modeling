from retrieve_objects import MinioRetriever, MinioUploader
import pandas as pd
from datetime import datetime
from utils import TOPIC_CONFIG

class RetrieveRecentPlays(MinioRetriever,MinioUploader):

    RAW_CONTAINER = "raw"
    PROCESSED_CONTAINER = "processed"

    def __init__(self, user, topic, raw, processed) -> None:
        MinioRetriever.__init__(self, user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)
        
        

    def get_user_recent_plays(self):
        tracks = []
        results = MinioRetriever.retrieve_object(self)
        print(f"results: {results}")
        for result in results:
            item = result["items"]
            track = item[0]['track']
            played_at = datetime.strptime(item[0]['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            
            tracks.append({
                'track_name': track['name'],
                'track_id': track['id'],
                'track_uri': track['uri'],
                'artist_name': track['artists'][0]['name'],
                'artist_id': track['artists'][0]['id'],
                'album_name': track['album']['name'],
                'album_id': track['album']['id'],
                'played_at': played_at,
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity']
            })
        # Convert to DataFrame
        df_tracks = pd.DataFrame(tracks)
        MinioUploader.upload_files(self,data=df_tracks)
        print("Object uploaded")
    

if __name__ == "__main__":
    ob = RetrieveRecentPlays("suhaas",TOPIC_CONFIG["recent_plays"]["topic"], "raw", "processed")
    ob.get_user_recent_plays()