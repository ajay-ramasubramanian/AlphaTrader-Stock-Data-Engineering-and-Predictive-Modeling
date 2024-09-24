import sql_queries
from ingestion.get_following_artist import run_retrieve_following_artists
from ingestion.get_liked_songs import run_retrieve_liked_songs
from ingestion.get_recent_plays import run_retrieve_recent_plays
from ingestion.get_saved_playlist import run_retrieve_saved_playlist
from ingestion.get_top_artists  import run_retrieve_top_artists
from ingestion.get_top_songs import run_retrieve_top_songs
from ingestion.get_artist_albums import run_get_user_artist_albums
from ingestion.get_related_artists import run_get_artist_related_artists


ingestion_task_configs = {
        'following_artists': run_retrieve_following_artists,
        'liked_songs': run_retrieve_liked_songs,
        'recent_plays': run_retrieve_recent_plays,
        'saved_playlists': run_retrieve_saved_playlist,
        'top_songs': run_retrieve_top_songs,
        'top_artists': run_retrieve_top_artists,
        'artist_albums': run_get_user_artist_albums,
        'related_artists': run_get_artist_related_artists
    }

create_table_task_config = {
        "artist": sql_queries.create_artist_table,
        "album": sql_queries.create_albums_table,
        "time": sql_queries.create_time_table,
        "track": sql_queries.create_all_tracks_table,
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
        "song_details": sql_queries.create_song_details
    }


insert_to_transformation_table_task_config = {
        'all_tracks_table': 'spotify-recent-plays-analysis', 
        'daily_plays': 'spotify-recent-plays-analysis', 
        'recent_tracks_by_popularity': 'spotify-recent-plays-analysis', 
        'recent_play_genre_analysis': 'spotify-recent-plays-analysis', 
        'recent_play_summary': 'spotify-recent-plays-analysis', 
        'recent_play_top_artists': 'spotify-recent-plays-analysis', 
        'artist_discovery': 'spotify-user-music-preferences', 
        'artist_frequency': 'spotify-user-music-preferences', 
        'genre_analysis': 'spotify-user-music-preferences', 
        'monthly_genre_trend': 'spotify-user-music-preferences', 
        'monthly_likes': 'spotify-user-music-preferences', 
        'song_details': 'spotify-user-music-preferences'
    }


insert_to_dim_table_task_config = {
        'dim_artist': 'spotify-related-artists',
        'dim_time': 'spotify-liked-songs',
        'dim_track': 'spotify-all-tracks',
        'dim_artist_genre_bridge': 'spotify-artist-genre-table',
        'dim_genre': 'spotify-genres-table',
        'dim_album': 'spotify-artist-genres'
    }


insert_to_fact_table_task_config = {
        'fact_liked_songs': 'spotify-liked-songs',
        'fact_recently_played': 'spotify-recent-plays',
    }
