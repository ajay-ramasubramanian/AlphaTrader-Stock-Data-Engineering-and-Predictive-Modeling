import os,site,sys
import numpy as np
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from processed.utils import TOPIC_CONFIG, MinioRetriever, MinioUploader


class RecentPlaysAnalysis:

    PROCESSED: str = 'processed'
    PRESENTATION: str = 'presentation'

    def __init__(self, user, table_1_topic,table_2_topic,table_3_topic) -> None:
        self.user = user
        self.retriever_1 = MinioRetriever(user=user, topic=table_1_topic, container=self.PROCESSED)
        self.retriever_2 = MinioRetriever(user=user, topic=table_2_topic, container=self.PROCESSED)
        self.uploader = MinioUploader(user=user, topic=table_3_topic, container = self.PRESENTATION)
    
    def retriever(self):
        recent_plays = self.retriever_1.retrieve_object()
        related_artist=self.retriever_2.retrieve_object()
        return recent_plays,related_artist
    

    
    def analyze_recent_plays_with_related(self,recent_plays, related_artist):
        # Ensure datetime format
        recent_plays['played_at'] = pd.to_datetime(recent_plays['played_at'])
        
        # Merge recent_plays with related_artist
        recent_with_related = recent_plays.merge(related_artist, left_on='artist_name', right_on='artist_name', how='left')
        
        # print(recent_with_related.columns)
        insights = []
        
        # Recent Plays Analysis
        recent_insights = pd.DataFrame({
            'metric': [
                'total_recent_listening_time',
                'avg_recent_song_duration',
                'unique_recent_tracks',
                'unique_recent_albums',
                'unique_recent_artists',
                'avg_recent_track_popularity'
            ],
            'value': [
                recent_plays['duration_ms'].sum() / (1000 * 60 * 60),          # Total listening time in hours
                recent_plays['duration_ms'].mean() / (1000 * 60 * 60),         # Average song duration in hours
                recent_plays['track_id'].nunique(),                            # Unique tracks played
                recent_plays['album_id'].nunique(),                            # Unique albums played
                recent_plays['artist_name'].nunique(),                         # Unique artists played
                recent_plays['popularity'].mean()                              # Average track popularity
            ]
        })

        print(f'recent_insights :{recent_insights.to_string(index=False)}')
        insights.append(recent_insights)
        
        # Top Recent Artists
        top_recent_artists = recent_with_related.groupby('artist_name').agg({
            'track_id': 'count',
            'artist_popularity': 'first'
        }).reset_index().sort_values('track_id', ascending=False).head(5)

        top_recent_artists.columns = ['artist_name', 'play_count', 'artist_popularity']
        print(top_recent_artists.columns)
        top_recent_artists['metric'] = ['top_recent_artist_' + str(i) for i in range(1, 6)]
        insights.append(top_recent_artists)
        
        # Recent Genre Analysis
        genre_counts = recent_with_related.explode('genres').groupby('genres').size().sort_values(ascending=False).head(5)
        genre_insights = pd.DataFrame({
            'metric': ['top_recent_genre_' + str(i + 1) for i in range(len(genre_counts))],
            'genre': genre_counts.index,
            'count': genre_counts.values
        })
        insights.append(genre_insights)
        
        # Listening Hours Analysis
        listening_hours = recent_plays['played_at'].dt.hour.value_counts().sort_index().reset_index()
        listening_hours.columns = ['hour', 'count']
        listening_hours['metric'] = 'listening_hour_' + listening_hours['hour'].astype(str)
        listening_hours = listening_hours[['metric', 'count']]
        
        insights.append(listening_hours)

        # Related Artists Analysis
        total_related_artists = len(related_artist)
        avg_related_artist_popularity = related_artist['artist_popularity'].mean()
        
        related_insights = pd.DataFrame({
            'metric': ['total_related_artists', 'avg_related_artist_popularity'],
            'value': [total_related_artists, avg_related_artist_popularity]
        })
        
        insights.append(related_insights)

        # Combine all insights into a single DataFrame
        all_insights = pd.concat(insights, ignore_index=True)

        # print(all_insights.to_string(index=False))
        
        return all_insights
    
    def write_to_parquet(self,df):
        self.uploader.upload_files(df)



if __name__ == "__main__":
    transformed = RecentPlaysAnalysis(user='suhaas',table_1_topic=TOPIC_CONFIG['recent_plays']['topic'], \
                                                table_2_topic=TOPIC_CONFIG['related_artists']['topic'], \
                                                table_3_topic=TOPIC_CONFIG['recent_plays_analysis']['topic'],)
    
    recent_plays, related_artists = transformed.retriever()
    recent_plays_analysis = transformed.analyze_recent_plays_with_related(recent_plays, related_artists)
    recent_plays_analysis.head()
    transformed.write_to_parquet(recent_plays_analysis)
# Usage example:
# recent_plays_insights = analyze_recent_plays_with_related(recent_plays, related_artist)