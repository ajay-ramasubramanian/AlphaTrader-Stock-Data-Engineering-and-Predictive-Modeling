import sys,os
import site
from datetime import datetime
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transformations.utils import MinioRetriever, MinioUploader
import pandas as pd
from ingestion.utils import TOPIC_CONFIG

class CreateGenresTable():

    def __init__(self, user, topic, processed, presentation) -> None:
        # print(processed)
        self.retriver = MinioRetriever(user, topic, processed)
        self.uploader = MinioUploader(user, 'genres_table', presentation)

    def get_artist_related_artists(self):
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

            df = df.reset_index(drop=True)
            
            self.uploader.upload_files(data=df)

        except ValueError as e:
            print(f"Encountered a value error here!!: {e}")

        
        print("object uploaded")
    

def run_get_artist_related_artists():
    ob = CreateGenresTable("suhaas", \
                                TOPIC_CONFIG["related_artists"]["topic"], \
                                "processed", \
                                "presentation")
    ob.get_artist_related_artists()

if __name__ == "__main__":
    run_get_artist_related_artists()