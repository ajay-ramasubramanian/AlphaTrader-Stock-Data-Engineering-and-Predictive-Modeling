from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd

class RetrieveLikedSongs(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,container) -> None:
        MinioRetriever.__init__(user, topic)
        MinioUploader.__init__(container,user,topic)
    def get_liked_songs(self):
        tracks = []
        all_data = MinioRetriever.retrieve_object()
        results= all_data[0]
        for item in results['items']:
            track = item['track']
            tracks.append({
                'name': track['name'],
                'artist': track['artists'][0]['name'],
                'album': track['album']['name'],
                'release_date': track['album']['release_date'],
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'id': track['id'],
                'uri': track['uri'],
                'added_at': item['added_at']
            })
        # Convert to DataFrame
        df_tracks= pd.DataFrame(tracks)
        MinioUploader.upload_files(data=df_tracks)
    

