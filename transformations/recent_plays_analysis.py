import os,site,sys
import numpy as np

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import pandas as pd
from transformations.utils import TOPIC_CONFIG, MinioRetriever, MinioUploader

load_dotenv()

class RecentPlaysAnalysis:

    PROCESSED: str = 'processed'
    PRESENTATION: str = 'presentation'

    def __init__(self, user, table_1_topic, table_2_topic, table_3_topic, host=os.getenv('HOST')) -> None:
        self.user = user
        self.retriever_1 = MinioRetriever(user=user, topic=table_1_topic, container=self.PROCESSED, host=host)
        self.retriever_2 = MinioRetriever(user=user, topic=table_2_topic, container=self.PROCESSED, host=host)
        self.uploader = MinioUploader(user=user, topic=table_3_topic, container = self.PRESENTATION, host=host)
    
    def retriever(self):
        recent_plays = self.retriever_1.retrieve_object()
        related_artist=self.retriever_2.retrieve_object()
        return recent_plays,related_artist
    

    
    def analyze_recent_plays(self, recent_plays):
        recent_plays['played_at'] = pd.to_datetime(recent_plays['played_at'])
        
        # Recent Plays Summary
        recent_summary = pd.DataFrame({
            'metric': [
                'total recent listening time',
                'avg recent song duration',
                'unique recent tracks',
                'unique recent albums',
                'unique recent artists',
                'avg recent track popularity'
            ],
            'value': [
                recent_plays['duration_ms'].sum() / (1000 * 60 * 60),
                recent_plays['duration_ms'].mean() / (1000 * 60),
                recent_plays['track_id'].nunique(),
                recent_plays['album_id'].nunique(),
                recent_plays['artist_name'].nunique(),
                recent_plays['popularity'].mean()
            ]
        })
    
        return recent_summary

    def analyze_top_recent_artists(self, recent_plays, related_artist):

        recent_with_related = recent_plays.merge(related_artist, on='artist_id', how='inner').rename(columns={'artist_name_x': 'artist_name'})
        
        top_recent_artists = recent_with_related.groupby('artist_name').agg({
            'track_id': 'count',
            'artist_popularity': 'first'
        }).reset_index().sort_values('track_id', ascending=False)
        
        top_recent_artists.columns = ['artist_name', 'play_count', 'artist_popularity']
        
        return top_recent_artists

    def analyze_recent_genres(self, recent_plays, related_artist):
        
        recent_with_related = recent_plays.merge(related_artist, on='artist_name', how='left')
        
        genre_counts = recent_with_related.explode('genres').groupby('genres').size().reset_index(name='count')
        
        genre_counts = genre_counts.sort_values('count', ascending=False)
        
        return genre_counts

    def analyze_listening_hours(self, recent_plays):
        
        listening_hours = recent_plays['played_at'].dt.hour.value_counts().sort_index().reset_index()
        
        listening_hours.columns = ['hour', 'count']
        
        return listening_hours

    def analyze_related_artists(self, related_artist):
        
        related_artist_summary = pd.DataFrame({
            'metric': ['total_related_artists', 'avg_related_artist_popularity'],
            'value': [len(related_artist), related_artist['artist_popularity'].mean()]
        })
        
        return related_artist_summary

    def analyze_recent_plays_daily(self, recent_plays):
        
        daily_plays = recent_plays.groupby(recent_plays['played_at'].dt.date).size().reset_index(name='play_count')
        
        daily_plays.columns = ['date', 'play_count']

        daily_plays.sort_values(by="date", ascending=True, inplace= True)
        
        return daily_plays

    def analyze_track_popularity(self, recent_plays):
        
        track_popularity = recent_plays[['track_name', 'artist_name', 'popularity']].drop_duplicates()
        
        track_popularity = track_popularity.sort_values('popularity', ascending=False)
        
        return track_popularity

    def analyze_recent_plays_with_related(self, recent_plays, related_artist):
        
        return {
            'recent-summary': self.analyze_recent_plays(recent_plays),
            'top-artists': self.analyze_top_recent_artists(recent_plays, related_artist),
            'genre-analysis': self.analyze_recent_genres(recent_plays, related_artist),
            'listening-hours': self.analyze_listening_hours(recent_plays),
            'related-artists-summary': self.analyze_related_artists(related_artist),
            'daily-plays': self.analyze_recent_plays_daily(recent_plays),
            'track-popularity': self.analyze_track_popularity(recent_plays)
        }
    
    def write_to_parquet(self,df,key):
        
        self.uploader.upload_files(df,key)


def recent_plays_analysis():
    transformed = RecentPlaysAnalysis(user='suhaas',table_1_topic=TOPIC_CONFIG['recent_plays']['topic'], \
                                                table_2_topic=TOPIC_CONFIG['related_artists']['topic'], \
                                                table_3_topic=TOPIC_CONFIG['recent_plays_analysis']['topic'],)
    
    recent_plays, related_artists = transformed.retriever()
    recent_plays_analysis = transformed.analyze_recent_plays_with_related(recent_plays, related_artists)
    for key in recent_plays_analysis:
        transformed.write_to_parquet(recent_plays_analysis[key], key)


if __name__ == "__main__":

    recent_plays_analysis()
    
