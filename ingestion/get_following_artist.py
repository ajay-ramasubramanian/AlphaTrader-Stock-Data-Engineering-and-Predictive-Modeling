import sys ,os
import site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from ingestion.utils import TOPIC_CONFIG
from ingestion.retrieve_objects import MinioRetriever,MinioUploader

class RetrieveFollowingArtists(MinioRetriever, MinioUploader):

    def __init__(self, user, topic, raw, processed) -> None:
        MinioRetriever.__init__(self, user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

    def get_user_followed_artists(self):
        artists = []
        results = MinioRetriever.retrieve_object(self)
        for result in results:
            # Process each artist
            # print(f"result: {result}")
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
        MinioUploader.upload_files(self,data=df_artists)
        print("object uploaded")
    

def run_retrieve_following_artists():
    ob = RetrieveFollowingArtists("suhaas", \
                                TOPIC_CONFIG["following_artists"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_user_followed_artists()

if __name__ == "__main__":
    run_retrieve_following_artists()