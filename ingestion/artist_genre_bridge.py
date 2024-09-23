import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformations.utils import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

from dotenv import load_dotenv
load_dotenv()

class ArtistGenreBridge():

    TOPIC = 'artist_genre_table'

    def __init__(self, user, artist_table, genre_table, processed, presentation) -> None:

        self.presentation = presentation
        self.retrieve_artists = MinioRetriever(user, artist_table, processed, os.getenv('HOST'))
        self.retrieve_genres = MinioRetriever(user, genre_table, presentation, os.getenv('HOST'))
        self.uploader = MinioUploader(user, self.TOPIC, presentation, os.getenv('HOST'))

        self.dtype_dict = {
            'artist_id': str,
            'genre_id': 'int64'
        }

    def create_artist_genre_bridge(self):
        artists_df = self.retrieve_artists.retrieve_object()
        genres_df = self.retrieve_genres.retrieve_object()

        try:
            bridge = []
            for _, row in artists_df.iterrows():
                for genre in row['genres']:
                    bridge.append({'artist_id': row['artist_id'], 'genre': genre})

            artists_genre_df = pd.DataFrame(bridge)

            bridge_df = artists_genre_df.merge(genres_df, how="inner", on="genre")
            bridge_df = bridge_df[['artist_id', 'genre_id']]
            bridge_df.drop_duplicates(inplace=True)

            bridge_df.astype(self.dtype_dict)
            bridge_df.reset_index(drop=True, inplace=True)
            print(bridge_df)
            
            self.uploader.upload_files(data=bridge_df)
            print(f"Successfully uploaded to '{self.presentation}' container!!")

        except ValueError as e:
            print(f"Encountered a value error here!!: {e}")

            

def run_get_artist_genre_bridge():
    ob = ArtistGenreBridge("suhaas", \
                                TOPIC_CONFIG["related_artists"]["topic"], \
                                "genres_table", \
                                "processed", \
                                "presentation")
    ob.create_artist_genre_bridge()

if __name__ == "__main__":
    run_get_artist_genre_bridge()