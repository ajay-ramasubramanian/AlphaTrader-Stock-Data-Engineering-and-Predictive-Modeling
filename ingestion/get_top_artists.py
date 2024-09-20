import sys,os
import site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG
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
        print(df_artists)
        MinioUploader.upload_files(self,data=df_artists)
        print("Object uploaded")
    

def run_retrieve_top_artists():
    ob = RetrieveTopArtists("suhaas", \
                            TOPIC_CONFIG["top_artists"]["topic"],\
                            "raw",\
                            "processed")
    ob.get_user_top_artists()


if __name__ == "__main__":
    run_retrieve_top_artists()


