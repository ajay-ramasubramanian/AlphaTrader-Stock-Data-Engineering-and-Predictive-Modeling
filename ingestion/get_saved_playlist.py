from retrieve_objects import MinioRetriever
import pandas as pd

class RetrieveRecentPlays(MinioRetriever):

    def __init__(self,user, topic) -> None:
        super().__init__(user, topic)

    def get_user_recent_plays(self):
        playlists = []
        all_data = super().retrieve_object()
        results= all_data[0]
        for item in results['items']:
            playlists.append({
                'playlist_name': item['name'],
                'playlist_id': item['id'],
                'playlist_uri': item['uri'],
                'owner_name': item['owner']['display_name'],
                'owner_id': item['owner']['id'],
                'is_public': item['public'],
                'is_collaborative': item['collaborative'],
                'total_tracks': item['tracks']['total'],
                'description': item['description']
            })
        # Convert to DataFrame
        df_playlist = pd.DataFrame(playlists)
        return df_playlist
    

