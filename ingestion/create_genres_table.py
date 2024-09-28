import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.retrieve_objects import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

from dotenv import load_dotenv
load_dotenv()

class CreateGenresTable():

    TOPIC = 'spotify_genres_table'

    def __init__(self, user, topic, raw, processed) -> None:

        self.retriver = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, self.TOPIC, processed)
        self.processed = processed

        self.dtype_dict = {
            'genre_id': 'int64',
            'genre': str
        }

    def create_genre_table(self):

        try:
            unique_genres = set()
            results = self.retriver.retrieve_object()
            for result in results:
                genres = result['genres']
                for genre in genres:
                    unique_genres.add(genre)

            df_dict = dict(zip(range(1, len(unique_genres)+1), unique_genres))
            df = pd.DataFrame(df_dict.items(), columns=['genre_id', 'genre'])

            df = df.astype(self.dtype_dict)
            df = df.reset_index(drop=True)
            
            self.uploader.upload_files(data=df)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except ValueError as e:
            print(f"Encountered a value error here!!: {e}")


def run_get_genre_table():
    ob = CreateGenresTable("suhaas", \
                                TOPIC_CONFIG["related_artists"]["topic"], \
                                "raw", \
                                "processed")
    ob.create_genre_table()

if __name__ == "__main__":
    run_get_genre_table()