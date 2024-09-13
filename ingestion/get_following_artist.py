from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd


class RetrieveFollowingArtists(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,container) -> None:
        MinioRetriever.__init__(user, topic)
        MinioUploader.__init__(container, user,topic)

    def get_user_followed_artists(self):
        artists = []
        all_data = MinioRetriever.retrieve_object()
        results= all_data[0]
        for result in results:
            # Process each artist
            for item in result['artists']['items']:
                artists.append({
                    'name': item['name'],
                    'id': item['id'],
                    'uri': item['uri'],
                    'popularity': item['popularity'],
                    'genres': ', '.join(item['genres']),
                    'followers': item['followers']['total']
                })
        # Convert to DataFrame
        df_artists = pd.DataFrame(artists)
        MinioUploader.upload_files(data=df_artists)
    
    