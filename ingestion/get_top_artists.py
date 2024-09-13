from retrieve_objects import MinioRetriever
import pandas as pd

class RetrieveTopArtists(MinioRetriever):

    def __init__(self,user, topic) -> None:
        super().__init__(user, topic)

    def get_user_top_artists(self):
        artists = []
        all_data = super().retrieve_object()
        results= all_data[0]
        for item in results['items']:
            artists.append({
                'artist_name': item['name'],
                'artist_id': item['id'],
                'artist_uri': item['uri'],
                'popularity': item['popularity'],
                'followers': item['followers']['total'],
                'genres': ', '.join(item['genres']),
                'image_url': item['images'][0]['url'] if item['images'] else None,
                'spotify_url': item['external_urls']['spotify'],
            })
        # Convert to DataFrame
        df_artists = pd.DataFrame(artists)
        return df_artists
    

