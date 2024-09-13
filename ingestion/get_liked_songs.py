from retrieve_objects import MinioRetriever
import pandas as pd

class RetrieveLikedSongs(MinioRetriever):

    def __init__(self,user, topic) -> None:
        super().__init__(user, topic)

    def get_liked_songs(self):
        tracks = []
        all_data = super().retrieve_object()
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
        return df_tracks
    

