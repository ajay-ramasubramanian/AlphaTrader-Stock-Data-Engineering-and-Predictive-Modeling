import os
import sys, site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transformations.utils import MinioRetriever,TOPIC_CONFIG
import pandas as pd


class ProcessTopAritstBasedOnGenres():

    PROCESSED: str = 'processed'
    PRESENTATION: str = 'presentation'

    def __init__(self, user, topic_1,topic_2) -> None:
        self.user = user
        self.topic_1 = topic_2
        self.retriever_1 = MinioRetriever(user=user, topic=topic_1, container=self.PROCESSED)
        self.retriever_2 = MinioRetriever(user=user, topic=topic_2, container=self.PROCESSED)

    def get_user_recent_plays(self):
        artists = []
        liked_songs = self.retriever_1.retrieve_object()
        related_artists=self.retriever_2.retrieve_object()
        print(liked_songs)
        print(related_artists)

        merged_df = pd.merge(liked_songs, related_artists, on="artist_id", how='inner')

        

        

        # Explode the genres column
        merged_df = merged_df.explode("genres")

        # Group by genres and aggregate
        genre_stats = merged_df.groupby('genres').agg({
            'artist_id': 'nunique',  # Count unique artists
            'artist_name': lambda x: ', '.join(set(x)),  # List unique artist names
            'artist_popularity': 'mean',  # Average popularity
            'artist_followers': 'sum'  # Total followers
        }).reset_index()

        # Rename columns
        genre_stats = genre_stats.rename(columns={
            'genres': 'Genre',
            'artist_id': 'Number of Artists',
            'artist_name': 'Artists',
            'artist_popularity': 'Avg Popularity',
            'artist_followers': 'Total Followers'
        })

        # Sort by number of artists and then by average popularity
        genre_stats = genre_stats.sort_values(['Number of Artists', 'Avg Popularity'], ascending=[False, False])

        # Display the results
        print(genre_stats.to_string(index=False))

        

        # Optionally, save the results back to a parquet file
        # result.to_parquet('top_artists_by_genre.parquet', index=False)

    
    
if __name__ == '__main__':
    ob = ProcessTopAritstBasedOnGenres('suhaas', TOPIC_CONFIG['liked_songs']['topic'],TOPIC_CONFIG['related_artists']['topic'])
    ob.get_user_recent_plays()
