import sys ,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
import pandas as pd
from ingestion.utils import TOPIC_CONFIG
from ingestion.retrieve_objects import MinioRetriever,MinioUploader

class RetrieveFollowingArtists():

    def __init__(self, user, topic, raw, processed) -> None:

        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, topic, processed)
        self.processed = processed

        self.dtype_dict = {
            'follow_id': 'int64',
            'artist_id': str
        }

    def get_user_followed_artists(self):

        try:

            artists = []
            results = self.retriever.retrieve_object()
            for count, result in enumerate(results):
                for item in result['artists']['items']:
                    artists.append({
                        'follow_id': count+1,
                        'artist_id': item['id']
                    })
            
            # Convert to DataFrame
            df_following_artist = pd.DataFrame(artists)
            df_following_artist['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")
            
            df_following_artist = df_following_artist.astype(self.dtype_dict)
            df_following_artist.drop_duplicates(inplace=True)
            df_following_artist = df_following_artist.reset_index(drop=True)
            
            self.uploader.upload_files(data=df_following_artist)
            print(f"Successfully uploaded to '{self.processed}' container!!")
        
        except Exception as e:
            print(f"Encountered an exception here!!: {e}")

    

def run_retrieve_following_artists():
    ob = RetrieveFollowingArtists("suhaas", \
                                TOPIC_CONFIG["following_artists"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_user_followed_artists()

if __name__ == "__main__":
    run_retrieve_following_artists()