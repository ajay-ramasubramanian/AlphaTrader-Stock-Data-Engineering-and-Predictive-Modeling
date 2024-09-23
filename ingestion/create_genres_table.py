import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformations.utils import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

from dotenv import load_dotenv
load_dotenv()

class CreateGenresTable():

    TOPIC = 'genres_table'

    def __init__(self, user, topic, processed, presentation) -> None:

        self.retriver = MinioRetriever(user, topic, processed, os.getenv('HOST'))
        self.uploader = MinioUploader(user, self.TOPIC, presentation, os.getenv('HOST'))
        self.presentation = presentation

        self.dtype_dict = {
            'genre_id': 'int64',
            'genre': str
        }

    def create_genre_table(self):
        
        df_related_artists = self.retriver.retrieve_object()
        try:
            all_genres = list(df_related_artists['genres'])
            unique_genres = set()
            for genres in all_genres:
                for genre in genres:
                    if genre not in unique_genres:
                        unique_genres.add(genre)

            df_dict = dict(zip(range(1, len(unique_genres)+1), unique_genres))
            df = pd.DataFrame(df_dict.items(), columns=['genre_id', 'genre'])

            df = df.astype(self.dtype_dict)
            df = df.reset_index(drop=True)
            
            self.uploader.upload_files(data=df)
            print(f"Successfully uploaded to '{self.presentation}' container!!")

        except ValueError as e:
            print(f"Encountered a value error here!!: {e}")
        
    

def run_get_genre_table():
    ob = CreateGenresTable("suhaas", \
                                TOPIC_CONFIG["related_artists"]["topic"], \
                                "processed", \
                                "presentation")
    ob.create_genre_table()

if __name__ == "__main__":
    run_get_genre_table()