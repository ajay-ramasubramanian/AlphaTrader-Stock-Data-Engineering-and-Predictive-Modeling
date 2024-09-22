import os
import sys, site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processed.utils import MinioRetriever,TOPIC_CONFIG, MinioUploader
import pandas as pd


class ProcessTopAritstBasedOnGenres():

    PROCESSED: str = 'processed'
    PRESENTATION: str = 'presentation'

    def __init__(self, user, table_1_topic,table_2_topic,table_3_topic) -> None:
        self.user = user
        self.retriever_1 = MinioRetriever(user=user, topic=table_1_topic, container=self.PROCESSED)
        self.retriever_2 = MinioRetriever(user=user, topic=table_2_topic, container=self.PROCESSED)
        self.uploader = MinioUploader(user=user, topic=table_3_topic, container = self.PRESENTATION)

    def top_songs_per_genre(self):
        
        # engine = create_engine('postgresql://airflow:airflow_pass@localhost:5433/airflow')
        artists = []
        liked_songs = self.retriever_1.retrieve_object()
        related_artists=self.retriever_2.retrieve_object()
        
        
        # Explode genres list into individual rows
        exploded_related_artists = related_artists.explode('genres')

        
        # Merge liked_songs with related_artist on artist_id to get artist and genre info for liked songs
        liked_with_genres = pd.merge(liked_songs, exploded_related_artists[['artist_id', 'artist_name', 'genres', 'artist_popularity']], on='artist_id', how='inner')
        
        print(liked_with_genres.columns)

        
        # Group by genre and artist_name, and calculate the average popularity per artist in each genre
        popular_artists_per_genre = liked_with_genres.groupby(['genres', 'artist_name']).agg(
            artist_popularity=('artist_popularity', 'mean'),
            track_count=('name', 'count')  # You can also count how many tracks each artist has in liked songs
        ).reset_index()

        popular_artists_per_genre = popular_artists_per_genre.sort_values(by=['track_count', 'artist_popularity'], ascending=[False, False])
        # Display the results
        # popular_artists_per_genre=popular_artists_per_genre.groupby(['genres']).apply(lambda x: x).reset_index(drop=True)
        print(popular_artists_per_genre.to_string(index=False))
        
        self.uploader.upload_files(popular_artists_per_genre)
        print("Uploaded ")

        

        # Optionally, save the results back to a parquet file

        # result.to_parquet('top_artists_by_genre.parquet', index=False)
def run_process_top_artist_based_on_genre():
    ob = ProcessTopAritstBasedOnGenres('suhaas', TOPIC_CONFIG['liked_songs']['topic'],\
                                        TOPIC_CONFIG['related_artists']['topic'], \
                                        TOPIC_CONFIG['top_songs_per_genre']['topic'])
    
    ob.top_songs_per_genre()
    
if __name__ == '__main__':
    run_process_top_artist_based_on_genre()
    
    







