import sys,os
import site
from datetime import datetime
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.retrieve_objects import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

class RetrieveRelatedArtists(MinioRetriever, MinioUploader):

    def __init__(self, user, topic, raw, processed) -> None:
        MinioRetriever.__init__(self, user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

        self.dtype_dict = {
            'artist_name': str,
            'artist_id': str,
            'artist_popularity': 'int64', # allows NaNs
            'genres': object,  # For lists or tuples
            'artist_followers': 'int64'
        }

    def get_artist_related_artists(self):
        results = MinioRetriever.retrieve_object(self)
        # Assuming 'results' is your list of artist dictionaries
        try:
            all_artists = []
            for result in results:
                all_artists.append({
                'artist_name': result['name'],
                'artist_id': result['id'],
                'artist_popularity': result['popularity'],
                'genres': result['genres'],
                'artist_followers': result['followers'],
                })
                
            df = pd.DataFrame(all_artists)
            df = df.astype(self.dtype_dict)

        except ValueError as e:
            print(f"Encountered a value error here!!: {e}")

        df['genres'] = df['genres'].apply(lambda x: list(x) if isinstance(x, tuple) else x)
        df['ingested_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df = df.fillna({
            'artist_name': '',
            'artist_id': '',
            'artist_popularity': 0,
            'genres': '',
            'artist_followers': 0
        })
        
        MinioUploader.upload_files(self, data=df)
        print("object uploaded")
    

def run_get_artist_related_artists():
    ob = RetrieveRelatedArtists("suhaas", \
                                TOPIC_CONFIG["related_artists"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_artist_related_artists()

if __name__ == "__main__":
    run_get_artist_related_artists()