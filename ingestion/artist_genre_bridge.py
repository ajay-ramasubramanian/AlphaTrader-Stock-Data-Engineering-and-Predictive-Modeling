import sys,os
import site
from datetime import datetime
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transformations.utils import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

class ArtistGenreBridge():

    def __init__(self, user, artist_table, genre_table, processed, presentation) -> None:
        self.retrieve_artists = MinioRetriever(user, artist_table, processed)
        self.retrieve_genres = MinioRetriever(user, genre_table, presentation)
        self.uploader = MinioUploader(user, 'artist_genre_table', presentation)

        # self.dtype_dict = {
        #     'artist_name': str,
        #     'artist_id': str,
        #     'artist_popularity': 'int64', # allows NaNs
        #     'genres': object, 
        #     'artist_followers': 'int64'
        # }

    def get_artist_related_artists(self):
        artists_df = self.retrieve_artists.retrieve_object()
        genres_df = self.retrieve_genres.retrieve_object()

        try:
            bridge = []
            for _, row in artists_df.iterrows():
                for genre in row['genres']:
                    bridge.append({'artist_id': row['artist_id'], 'genre': genre})

            bridge_df = pd.DataFrame(bridge)

            artists_genre_df = bridge_df.merge(genres_df, how="inner", on="genre")
            artists_genre_df = artists_genre_df[['artist_id', 'genre_id']]

            # print(artists_genre_df.head(10))
            
            self.uploader.upload_files(data=artists_genre_df)

        except ValueError as e:
            print(f"Encountered a value error here!!: {e}")

        
        # print("object uploaded")
    

def run_get_artist_related_artists():
    ob = ArtistGenreBridge("suhaas", \
                                TOPIC_CONFIG["related_artists"]["topic"], \
                                "genres_table", \
                                "processed", \
                                "presentation")
    ob.get_artist_related_artists()

if __name__ == "__main__":
    run_get_artist_related_artists()