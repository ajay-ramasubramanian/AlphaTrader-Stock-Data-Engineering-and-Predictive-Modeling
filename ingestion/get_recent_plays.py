import sys,os
import site
from datetime import datetime
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG

class RetrieveRecentPlays(MinioRetriever,MinioUploader):

    def __init__(self, user, topic, raw, processed) -> None:
        MinioRetriever.__init__(self, user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

    def get_user_recent_plays(self):
        tracks = []
        results = MinioRetriever.retrieve_object(self)
        for result in results:
            for item in result["items"]:
                track = item['track']
                played_at = datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                
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
        df_tracks['ingested_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        MinioUploader.upload_files(self,data=df_tracks)
        print("Object uploaded")
    
def run_retrieve_recent_plays():
    ob = RetrieveRecentPlays("suhaas", \
                            TOPIC_CONFIG["recent_plays"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_user_recent_plays()


if __name__ == "__main__":
    run_retrieve_recent_plays()
    