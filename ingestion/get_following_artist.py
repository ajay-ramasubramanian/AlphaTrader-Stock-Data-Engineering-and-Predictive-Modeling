from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd
from utils import TOPIC_CONFIG

class RetrieveFollowingArtists(MinioRetriever, MinioUploader):

    def __init__(self, user, topic, raw, processed) -> None:
        MinioRetriever.__init__(self, user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

    def get_user_followed_artists(self):
        artists = []
        results = MinioRetriever.retrieve_object(self)
        for result in results:
            # Process each artist
            print(f"result: {result}")
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
    

if __name__ == "__main__":
    ob = RetrieveFollowingArtists("suhaas", \
                                TOPIC_CONFIG["following_artists"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_user_followed_artists()
