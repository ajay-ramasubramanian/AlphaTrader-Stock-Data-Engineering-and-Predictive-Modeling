import sys
from pathlib import Path

project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

import pandas as pd
from transformations.utils import MinioRetriever
import psycopg2
from io import StringIO


def gold_to_warehouse():

    df = MinioRetriever('suhaas', 'spotify-all-tracks', 'processed', 'minio').retrieve_object()
    table_name = 'dim_track'

    # Reads all_tack.parquet file from MinIO object store
    df_all_tracks = MinioRetriever('suhaas', 'spotify-all-tracks', 'processed', 'minio').retrieve_object()
    df_all_tracks = df_all_tracks.drop_duplicates('track_id', keep='first')

    print(len(df_all_tracks['track_id']))
    print(len(df_all_tracks['track_id'].unique()))
    

    # Recent plays analysis
    # Reads daily_plays.parquet file from MinIO object store
    df_daily_tracks = MinioRetriever('suhaas', 'spotify-recent-plays-analysis', 'presentation', 'minio').retrieve_object(key='daily_plays')

    # Reads genre_analysis.parquet file from MinIO object store
    df_recently_played_genre_analysis = MinioRetriever('suhaas', 'spotify-recent-plays-analysis', 'presentation', 'mino').retrieve_object(key='genre_analysis')

    # Reads track_popularity.parquet file from MinIO object store
    df_recently_played_track_popularity = MinioRetriever('suhaas', 'spotify-recent-plays-analysis', 'presentation', 'mino').retrieve_object(key='track_popularity')

    # Reads recent_summary.parquet file from MinIO object store
    df_recent_summary = MinioRetriever('suhaas', 'spotify-recent-plays-analysis', 'presentation', 'mino').retrieve_object(key='recent_summary')

    # Read  top_artists.parquet file form MinIO object store
    df_recently_played_top_artist = MinioRetriever('suhaas', 'spotify-recent-plays-analysis', 'presentation', 'minio').retrieve_object(key='top_artists')


    # User-music-preferences
    # Reads artist_discovery.parquet file from MinIO object store
    df_artist_discovery = MinioRetriever('suhaas', 'spotify-user-music-preferences', 'presentation', 'minio').retrieve_object(key='artist-discovery')

    # Reads artist_frequency.parquet file from MinIO object store
    df_artist_frequency = MinioRetriever('suhaas', 'spotify-user-music-preferences', 'presentation', 'minio').retrieve_object(key='artist-frequency')

    # Reads genre_analysis.parquet file from MinIO object store
    df_user_genre_analysis = MinioRetriever('suhaas', 'spotify-user-music-preferences', 'presentation', 'minio').retrieve_object(key='genre-analysis')

    # Reads monthly_genre_trend.parquet file from MinIO object store
    df_monthly_genre_trend = MinioRetriever('suhaas', 'spotify-user-music-preferences', 'presentation', 'minio').retrieve_object(key='monthly-genre-trend')

    # Reads monthly_likes.parquet file from MinIO object store
    df_monthly_likes = MinioRetriever('suhaas', 'spotify-user-music-preferences', 'presentation', 'minio').retrieve_object(key='monthly-likes')

    # Reads song_details.parquet file from MinIO object store
    df_song_details = MinioRetriever('suhaas', 'spotify-user-music-preferences', 'presentation', 'minio').retrieve_object(key='song-details')


    import psycopg2
    from io import StringIO
    
    db_params = {
    "dbname": "spotify_db",
    "user": "spotify",
    "password": "spotify_pass",
    "host": "postgres-warehouse"
    }

    table_df_dict = {
    "create_all_tracks_table": df_all_tracks,
    # Recent plays analysis
    "create_daily_plays": df_all_tracks,
    "create_recent_tracks_by_popularity": df_recently_played_track_popularity,
    "create_recent_play_genre_analysis": df_recently_played_genre_analysis,
    "create_recent_play_summary": df_recent_summary,
    "create_recent_play_top_artists": df_recently_played_top_artist,
    
    # User music preferences
    "create_artist_discovery": df_artist_discovery,
    "create_artist_frequency": df_artist_frequency,
    "create_genre_analysis" : df_user_genre_analysis,
    "create_monthly_genre_trend": df_monthly_genre_trend,
    "create_monthly_likes": df_monthly_likes,
    "create_song_details": df_song_details

    # Add more tables and DataFrames as needed
}
    
    def bulk_insert(conn, df, table_name):
        cur = conn.cursor()
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, table_name, null="")
        conn.commit()
        cur.close()
    
    def insert_multiple_tables(db_params, table_df_dict):
        conn = psycopg2.connect(**db_params)
        try:
            for table_name, df in table_df_dict.items():
                bulk_insert(conn, df, table_name)
            print("All data inserted successfully")
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            conn.close()
    
    insert_multiple_tables(db_params,table_df_dict)


# gold_to_warehouse()