import sys,os
import site
from datetime import datetime
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

class RetrieveArtistAlbums(MinioRetriever, MinioUploader):

    def __init__(self, user, topic, raw, processed) -> None:
        MinioRetriever.__init__(self, user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

        self.dtype_dict = {
            'album_name': str,
            'album_id': str,
            'album_type': str, # allows NaNs
            'total_tracks': 'int64', 
            'release_date': str,
            'artist_name': str,
            'artist_id': str
        }

    def get_user_artist_albums(self):
        artists = []
        results = MinioRetriever.retrieve_object(self)
        
        for result in results:
            artists.append({
                'album_name': result['name'],
                'album_id': result['id'],
                'album_type': result['album_type'],
                'total_tracks': result['total_tracks'],
                'release_date': result['release_date'],
                'artist_name': result['artists'][0]['name'],
                'artist_id': result['artists'][0]['id'],
                
            })
        # Convert to DataFrame
        df_artists = pd.DataFrame(artists)
        df_artists['ingested_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_artists = df_artists.astype(self.dtype_dict)

        df_artists.drop_duplicates(['album_id', 'artist_id'], inplace=True)
        df_artists['release_date'] = pd.to_datetime(df_artists['release_date'])
        
        MinioUploader.upload_files(self,data=df_artists)
        print("object uploaded")
    
def run_get_user_artist_albums():
    ob = RetrieveArtistAlbums("suhaas", \
                                TOPIC_CONFIG["artist_albums"]["topic"], \
                                "raw", \
                                "processed")
    ob.get_user_artist_albums()


if __name__ == "__main__":
    run_get_user_artist_albums()

