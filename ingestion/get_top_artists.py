from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd
from utils import TOPIC_CONFIG
class RetrieveTopArtists(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,raw, processed) -> None:
        MinioRetriever.__init__(self,user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

    def get_user_top_artists(self):
        artists = []
        results = MinioRetriever.retrieve_object(self)
        for result in results:
            item = result["items"][0]
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
        MinioUploader.upload_files(self,data=df_artists)
        print("Object uploaded")
    


if __name__ == "__main__":
    ob = RetrieveTopArtists("suhaas", \
                            TOPIC_CONFIG["top_artists"]["topic"],\
                            "raw",\
                            "processed")
    ob.get_user_top_artists()
    

