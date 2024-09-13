from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd

class RetrieveSavedPlaylist(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,container) -> None:
        MinioRetriever.__init__(user, topic)
        MinioUploader.__init__(container, user, topic)

    def get_user_recent_plays(self):
        playlists = []
        all_data = MinioRetriever.retrieve_object()
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
        MinioUploader.upload_files(data=df_playlist)

    

