from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd
from datetime import datetime

class RetrieveRecentPlays(MinioRetriever,MinioUploader):

    def __init__(self,user, topic, container) -> None:
        MinioRetriever.__init__(user, topic)
        MinioUploader.__init__(container, user,topic)

    def get_user_recent_plays(self):
        tracks = []
        all_data = MinioRetriever.retrieve_object()
        results= all_data[0]
        for item in results['items']:
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
        MinioUploader.upload_files(data=df_tracks)
    

