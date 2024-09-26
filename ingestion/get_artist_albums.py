import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

class RetrieveArtistAlbums(MinioRetriever, MinioUploader):

    TOPIC = 'spotify_artist_genres'

    def __init__(self, user, topic, raw, processed) -> None:

        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, self.TOPIC, processed)
        self.processed = processed

        self.dtype_dict = {
            'album_name': str,
            'album_id': str,
            'album_type': str, # allows NaNs
            'total_tracks': 'int64', 
            'release_date': object,
            'artist_name': str,
            'artist_id': str,
            'ingested_on': str
        }

    def get_user_artist_albums(self):

        artists = []
        try: 

            results = self.retriever.retrieve_object()
            for result in results:
                artists.append({
                    'album_id': result['id'],
                    'album_name': result['name'],
                    'album_type': result['album_type'],
                    'total_tracks': result['total_tracks'],
                    'release_date': result['release_date'],
                    'artist_id': result['artists'][0]['id'],
                    'artist_name': result['artists'][0]['name']
                })

            # Convert to DataFrame
            df_artists = pd.DataFrame(artists)
            df_artists['release_date'] = pd.to_datetime(df_artists['release_date'])
            df_artists['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")
            
            df_artists = df_artists.astype(self.dtype_dict)
            df_artists.drop_duplicates(['album_id', 'artist_id'], inplace=True)
            df_artists = df_artists.reset_index(drop=True)

            self.uploader.upload_files(data=df_artists)
            print(f"Successfully uploaded to '{self.processed}' container!!")
        
        except Exception as e:
            print(f"Encountered an exception here!!: {e}")
    
def run_get_user_artist_albums():
    ob = RetrieveArtistAlbums("suhaas", \
                                TOPIC_CONFIG["artist_albums"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_user_artist_albums()


if __name__ == "__main__":
    run_get_user_artist_albums()

