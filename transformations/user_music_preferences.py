
import os,site,sys
import numpy as np
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from transformations.utils import TOPIC_CONFIG, MinioRetriever, MinioUploader


class ProcessTopAritstBasedOnGenres:

    PROCESSED: str = 'processed'
    PRESENTATION: str = 'presentation'

    def __init__(self, user, table_1_topic,table_2_topic,table_3_topic) -> None:
        self.user = user
        self.retriever_1 = MinioRetriever(user=user, topic=table_1_topic, container=self.PROCESSED)
        self.retriever_2 = MinioRetriever(user=user, topic=table_2_topic, container=self.PROCESSED)
        self.uploader = MinioUploader(user=user, topic=table_3_topic, container = self.PRESENTATION)
    
    def retriever(self):
        liked_songs = self.retriever_1.retrieve_object()
        related_artists=self.retriever_2.retrieve_object()
        return liked_songs,related_artists


    def transform_liked_songs_related_artists(self,liked_songs, related_artists):
        # Ensure datetime format for 'added_at'
        liked_songs['added_at'] = pd.to_datetime(liked_songs['added_at'])

        # Merge liked_songs with related_artists
        merged_df = liked_songs.merge(related_artists, on='artist_id', how='left')

        # 1. Genre Analysis
        genre_counts = (
            merged_df.explode('genres')
            .groupby('genres')
            .size()
            .sort_values(ascending=False)
            .reset_index(name='genre_count')
        )

        # 2. Artist Discovery
        liked_artists = set(liked_songs['artist_id'])
        new_artists = related_artists[~related_artists['artist_id'].isin(liked_artists)]
        new_artists_ranked = new_artists.sort_values('artist_popularity', ascending=False)

        # 3. Popularity Analysis
        avg_liked_popularity = liked_songs['popularity'].mean()
        avg_related_popularity = related_artists['artist_popularity'].mean()

        # 4. Time-based Analysis
        liked_songs['month_year'] = liked_songs['added_at'].dt.to_period('M')
        print(f"like songs : {liked_songs}")
        monthly_likes = liked_songs.groupby('month_year').size().reset_index(name='monthly_like_count')
        
        #5. Artist Frequency and Related Artists Analysis
        top_liked_artists = merged_df['artist_name'].value_counts().head(10)
        # print(f'top liked artist :{top_liked_artists}')
        top_liked_artists_id = liked_songs['artist_id'].value_counts().head(10)
        related_to_top = related_artists[related_artists['artist_id'].isin(top_liked_artists_id.index)]
        
        # Calculate average popularity of related artists for top liked artists
        avg_related_popularity = related_to_top['artist_popularity'].mean()
        
        # Find most common genre among related artists
        all_genres = [genre for genres in related_to_top['genres'] for genre in genres]
        most_common_related_genre = max(set(all_genres), key=all_genres.count)

        liked_songs_insights = pd.DataFrame({
            'total_liked_songs': [len(liked_songs)],
            'unique_artists_in_liked_songs': [liked_songs['artist_id'].nunique()],
            'avg_song_popularity': [avg_liked_popularity],
            'avg_related_artist_popularity': [avg_related_popularity],
            'top_genre': [genre_counts.iloc[0]['genres']],
            'top_genre_count': [genre_counts.iloc[0]['genre_count']],
            'new_artists_to_discover': [len(new_artists)],
            'top_new_artist_to_discover': [new_artists_ranked.iloc[0]['artist_name']],
            'top_new_artist_popularity': [new_artists_ranked.iloc[0]['artist_popularity']],
            'most_liked_artist': [top_liked_artists.index[0]],
            'most_liked_artist_count': [top_liked_artists_id.iloc[0]],
            'oldest_liked_song_date': [liked_songs['added_at'].min()],
            'newest_liked_song_date': [liked_songs['added_at'].max()],
            'avg_song_duration_ms': [liked_songs['duration_ms'].mean()],
            'avg_top_related_artist_popularity': [avg_related_popularity],
            'most_common_related_genre': [most_common_related_genre],
            'num_related_artists_for_top_liked': [len(related_to_top)]
        })

        # top 5 genres
        for i, (genre, count) in enumerate(genre_counts.head().itertuples(index=False), 1):
            liked_songs_insights[f'top_genre_{i}'] = genre
            liked_songs_insights[f'top_genre_{i}_count'] = count

        # monthly like counts for the last 12 months
        last_12_months = monthly_likes.sort_values('month_year', ascending=False).head(12)
        for i, (month, count) in enumerate(last_12_months.itertuples(index=False), 1):
            liked_songs_insights[f'month_{i}_likes'] = count
            liked_songs_insights[f'month_{i}'] = str(month)

        # top 5 related artists for the most liked artist
        top_related = related_to_top[related_to_top['artist_id'] == top_liked_artists_id.index[0]].sort_values('artist_popularity', ascending=False).head()
        for i, (name, popularity) in enumerate(zip(top_related['artist_name'], top_related['artist_popularity']), 1):
            liked_songs_insights[f'top_related_artist_{i}'] = name
            liked_songs_insights[f'top_related_artist_{i}_popularity'] = popularity
        
        # print(liked_songs_insights.to_string(index=False))

        return liked_songs_insights
    

    def write_to_parquet(self,df):
        self.uploader.upload_files(df)
# Usage

if __name__ == "__main__":
    transformed = ProcessTopAritstBasedOnGenres(user='suhaas',table_1_topic=TOPIC_CONFIG['liked_songs']['topic'], \
                                                table_2_topic=TOPIC_CONFIG['related_artists']['topic'], \
                                                table_3_topic=TOPIC_CONFIG['user_music_preferences']['topic'],)
    
    liked_songs, related_artists = transformed.retriever()
    user_music_preferences = transformed.transform_liked_songs_related_artists(liked_songs, related_artists)
    user_music_preferences.head()
    transformed.write_to_parquet(user_music_preferences)