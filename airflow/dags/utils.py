import sql_queries

from ingestion.artist_genre_bridge import run_get_artist_genre_bridge
from ingestion.create_genres_table import run_get_genre_table
from ingestion.create_time_table import run_create_time_table
from ingestion.get_all_tracks import run_retrieve_all_tracks
from ingestion.get_artist_albums import run_get_user_artist_albums
from ingestion.get_following_artist import run_retrieve_following_artists
from ingestion.get_liked_songs import run_retrieve_liked_songs
from ingestion.get_recent_plays import run_retrieve_recent_plays
from ingestion.get_related_artists import run_get_artist_related_artists
from ingestion.get_saved_playlist import run_retrieve_saved_playlist
from ingestion.get_top_artists import run_retrieve_top_artists
from ingestion.get_top_songs import run_retrieve_top_songs
from transformations.recent_plays_analysis import recent_plays_analysis
from transformations.source_tables import (
    processed_to_presentation_all_tracks,
    processed_to_presentation_artist_albums,
    processed_to_presentation_genres_table,
    processed_to_presentation_liked_songs,
    processed_to_presentation_recent_plays,
    processed_to_presentation_related_artists,
    processed_to_presentation_top_songs
    )
from transformations.user_music_preferences import user_music_preferences
from data_checks.ingestion.expectations import create_ingestion_expectation_suites
from data_checks.postgres.expectations import create_postgres_expectation_suites


independent_ingestion_task_configs = {
        'following_artists': run_retrieve_following_artists,
        'liked_songs': run_retrieve_liked_songs,
        'recent_plays': run_retrieve_recent_plays,
        'saved_playlists': run_retrieve_saved_playlist,
        'top_songs': run_retrieve_top_songs,
        'top_artists': run_retrieve_top_artists,
        'artist_albums': run_get_user_artist_albums,
        'related_artists': run_get_artist_related_artists,
        'all_tracks': run_retrieve_all_tracks,
        'genres_table': run_get_genre_table,

    }

dependent_ingestion_task_configs = {
    'time_table': run_create_time_table,
    'artist_genre_bridge': run_get_artist_genre_bridge,
}

process_to_presentation_task_configs = {
    'recent_plays': processed_to_presentation_recent_plays,
    'liked_songs': processed_to_presentation_liked_songs,
    'related_artists': processed_to_presentation_related_artists,
    'all_tracks': processed_to_presentation_all_tracks,
    'artist_albums': processed_to_presentation_artist_albums,
    'genres_table': processed_to_presentation_genres_table,
    'top_songs': processed_to_presentation_top_songs
}

transformation_task_configs = {
    'recent_plays_analysis': recent_plays_analysis,
    'user_music_preferences': user_music_preferences
}

create_table_task_configs = {
        "artist": sql_queries.create_artist_table,
        "album": sql_queries.create_albums_table,
        "time": sql_queries.create_time_table,
        "track": sql_queries.create_all_tracks_table,
        "genre": sql_queries.create_genres_table,
        "artist_genre_bridge": sql_queries.create_artist_genre_bridge,
        "liked_songs": sql_queries.create_liked_songs_table,
        "recently_played": sql_queries.create_recently_played_table,
        "all_tracks_table": sql_queries.create_all_tracks_table,
        "daily_plays": sql_queries.create_daily_plays,
        "recent_tracks_by_popularity": sql_queries.create_recent_tracks_by_popularity,
        "recent_play_genre_analysis": sql_queries.create_recent_play_genre_analysis,
        "recent_play_summary": sql_queries.create_recent_play_summary,
        "recent_play_top_artists": sql_queries.create_recent_play_top_artists,
        "artist_discovery": sql_queries.create_artist_discovery,
        "artist_frequency": sql_queries.create_artist_frequency,
        "genre_analysis": sql_queries.create_genre_analysis,
        "monthly_genre_trend": sql_queries.create_monthly_genre_trend,
        "monthly_likes": sql_queries.create_monthly_likes,
        "song_details": sql_queries.create_song_details,
        "user_details": sql_queries.create_user_details_table,
        "top_songs" : sql_queries.create_top_songs
    }


create_data_checks_task_configs = {
    'ingestion_expectation_suites': create_ingestion_expectation_suites, 
    'final_expectation_suites': create_postgres_expectation_suites
}

insert_to_transformation_table_task_configs = {
        'daily_plays': {'topic':'spotify-recent-plays-analysis',
                        'key': 'daily-plays'},
        'recent_tracks_by_popularity': {'topic':'spotify-recent-plays-analysis',
                                        'key':'track-popularity'},
        'recent_plays_genre_analysis': {'topic':'spotify-recent-plays-analysis',
                                    'key': 'genre-analysis'},
        'recent_plays_summary': {'topic':'spotify-recent-plays-analysis',
                                'key': 'recent-summary'},
        'recent_plays_top_artists': {'topic':'spotify-recent-plays-analysis',
                                    'key': 'top-artists'},
        'artist_discovery': {'topic':'spotify-user-music-preferences',
                            'key': 'artist-discovery'},
        'artist_frequency': {'topic':'spotify-user-music-preferences',
                            'key': 'artist-frequency'},
        'genre_analysis': {'topic':'spotify-user-music-preferences',
                        'key': 'genre-analysis'} ,
        'monthly_genre_trend': {'topic':'spotify-user-music-preferences',
                                'key': 'monthly-genre-trends'},
        'monthly_likes': {'topic':'spotify-user-music-preferences',
                        'key': 'monthly-likes'},
        'song_details': {'topic':'spotify-user-music-preferences',
                        'key': 'song-details'}
    }


insert_to_dim_table_task_configs = {
        'dim_artist': 'spotify-related-artists',
        'dim_time': 'spotify-time-table',
        'dim_track': 'spotify-all-tracks',
        'dim_artist_genre_bridge': 'spotify-artist-genre-table',
        'dim_genre': 'spotify-genres-table',
        'dim_album': 'spotify-artist-albums',
        'dim_top_songs': 'spotify-top-songs'
    }


insert_to_fact_table_task_configs = {
        'fact_liked_songs': 'spotify-liked-songs',
        'fact_recently_played': 'spotify-recent-plays',
        'user_details': 'spotify-user-details'
    }

