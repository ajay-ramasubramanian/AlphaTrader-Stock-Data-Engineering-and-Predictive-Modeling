
import os,site,sys
import numpy as np
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from transformations.utils import TOPIC_CONFIG, MinioRetriever, MinioUploader


class ProcessTopAritstBasedOnGenres:

    PROCESSED: str = 'processed'
    PRESENTATION: str = 'presentation'

    def __init__(self, user, table_1_topic,table_2_topic,table_3_topic, table_4_topic) -> None:
        self.user = user
        self.retriever_1 = MinioRetriever(user=user, topic=table_1_topic, container=self.PROCESSED,host="minio")
        self.retriever_2 = MinioRetriever(user=user, topic=table_2_topic, container=self.PROCESSED,host="minio")
        self.retriever_3 = MinioRetriever(user=user, topic=table_3_topic, container=self.PROCESSED,host="minio")
        self.uploader = MinioUploader(user=user, topic=table_4_topic, container = self.PRESENTATION,host="minio")
    
    def retriever(self):
        liked_songs = self.retriever_1.retrieve_object()
        related_artists=self.retriever_2.retrieve_object()
        all_tracks = self.retriever_3.retrieve_object()
        return liked_songs,related_artists, all_tracks


    def transform_liked_songs_related_artists(self, liked_songs, related_artists, all_tracks):
    # Merge dataframes
        all_tracks_merged = liked_songs.merge(all_tracks, on='track_id', how='inner')
        merged_df = all_tracks_merged.merge(related_artists, on='artist_id', how='left')

        # Genre Analysis
        genre_df = (
            merged_df.explode('genres')
            .groupby('genres')
            .size()
            .sort_values(ascending=False)
            .reset_index(name='genre_count')
        )

        # Artist Discovery
        liked_artists = set(liked_songs['artist_id'])
        new_artists_df = related_artists[~related_artists['artist_id'].isin(liked_artists)]
        new_artists_df = new_artists_df.sort_values('artist_popularity', ascending=False)

        # Popularity Analysis
        popularity_df = merged_df[['artist_name_x', 'artist_popularity']].drop_duplicates()
        popularity_df = popularity_df.rename(columns={'artist_name_x': 'artist_name'})

        # Time-based Analysis
        liked_songs['month_year'] = pd.to_datetime(liked_songs['added_at']).dt.to_period('M')
        monthly_likes_df = liked_songs.groupby('month_year').size().reset_index(name='monthly_like_count')
        monthly_likes_df['month_year'] = monthly_likes_df['month_year'].astype(str)

        # Artist Frequency Analysis
        artist_frequency_df = merged_df['artist_name_x'].value_counts().reset_index()
        artist_frequency_df.columns = ['artist_name', 'like_count']

        # Song Details
        song_details_df = merged_df[['track_name', 'artist_name_x', 'album_name', 'duration_ms', 'artist_popularity', 'added_at']]
        song_details_df = song_details_df.rename(columns={'artist_name_x': 'artist_name'})

        # Artist Genre Mapping
        artist_genre_df = merged_df[['artist_name_x', 'genres']].explode('genres').drop_duplicates()
        artist_genre_df = artist_genre_df.rename(columns={'artist_name_x': 'artist_name'})

        # Monthly Genre Trends
        monthly_genre_df = merged_df.explode('genres')
        monthly_genre_df['month_year'] = pd.to_datetime(monthly_genre_df['added_at']).dt.to_period('M')
        monthly_genre_df = monthly_genre_df.groupby(['month_year', 'genres']).size().reset_index(name='genre_count')
        monthly_genre_df['month_year'] = monthly_genre_df['month_year'].astype(str)

        return {
            'genre_analysis': genre_df,
            'artist_discovery': new_artists_df,
            'popularity_analysis': popularity_df,
            'monthly_likes': monthly_likes_df,
            'artist_frequency': artist_frequency_df,
            'song_details': song_details_df,
            'artist_genre_mapping': artist_genre_df,
            'monthly_genre_trends': monthly_genre_df
        }
    

    def write_to_parquet(self,df,key):
        self.uploader.upload_files(df,key)
# Usage

if __name__ == "__main__":
    transformed = ProcessTopAritstBasedOnGenres(user='suhaas',table_1_topic=TOPIC_CONFIG['liked_songs']['topic'], \
                                                table_2_topic=TOPIC_CONFIG['related_artists']['topic'], \
                                                table_3_topic=TOPIC_CONFIG['all_tracks']['topic'], \
                                                table_4_topic=TOPIC_CONFIG['user_music_preferences']['topic'],)
    
    liked_songs, related_artists, all_tracks= transformed.retriever()
    user_music_preferences = transformed.transform_liked_songs_related_artists(liked_songs, related_artists, all_tracks)

    for key in user_music_preferences:
        transformed.write_to_parquet(user_music_preferences[key], key)
    
    
