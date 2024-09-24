## make dimension tables: artist, album, genre, tracks, time, 

## Dimension tables
create_artist_table = """
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id VARCHAR(22) PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL,
    artist_popularity SMALLINT,
    artist_followers INTEGER
)
"""

create_time_table = """
CREATE TABLE IF NOT EXISTS dim_time (
    date_id DATE PRIMARY KEY,
    year SMALLINT  NOT NULL,
    month SMALLINT  NOT NULL,
    day SMALLINT  NOT NULL,
    hour SMALLINT,
    minute SMALLINT, 
    second SMALLINT 
)
"""

create_albums_table = """
CREATE TABLE IF NOT EXISTS dim_album (
    album_id VARCHAR(50) PRIMARY KEY,
    album_name VARCHAR(50) NOT NULL,
    album_type VARCHAR(50),
    total_tracks SMALLINT, 
    release_date VARCHAR(50),
    artist_name VARCHAR(50),
    artist_id VARCHAR(22)
)
"""

create_all_tracks_table = """
CREATE TABLE IF NOT EXISTS dim_track (
    track_id VARCHAR(256) PRIMARY KEY,
    track_name VARCHAR(256) NOT NULL,
    duration_ms INTEGER,
    track_popularity INTEGER,
    track_uri VARCHAR(256),
    artist_name VARCHAR(256) NOT NULL,
    album_name VARCHAR(256)
)
"""


## Fact tables
create_liked_songs_table = """
CREATE TABLE IF NOT EXISTS fact_liked_songs (
    like_id SMALLINT PRIMARY KEY,
    artist_id VARCHAR(22) NOT NULL,
    album_id VARCHAR(22),
    track_id VARCHAR(22) NOT NULL,
    time_id VARCHAR(22) NOT NULL,
    added_at TIMESTAMP
)
"""


#Tables for dashboarding:

# User music preferences
create_artist_discovery = """
CREATE TABLE IF NOT EXISTS artist_discovery (
    artist_name VARCHAR(255) NOT FULL,
    artist_id BIGINT PRIMARY KEY NOT NULL,
    artist_populartiy SMALLINT,
    genres VARCHAR(22),
    artist_followers BIGINT,
    ingested_on TIMESTAMP
)
"""

create_artist_frequency = """
CREATE TABLE IF NOT EXISTS artist_frequency(
    artist_name VARCHAR(22) NOT NULL,
    like_count SMALLINT
)
"""

create_genre_analysis = """
CREATE TABLE IF NOT EXISTS genre_analysis(
    genres VARCHAR(22) NOT NULL,
    genre_count SMALLINT
)
"""

create_monthly_genre_trend = """
CREATE TABLE IF NOT EXISTS monthly_genre_trend(
    month_year DATE NOT NULL,
    genres VARCHAR(22) NOT NULL,
    genre_count SMALLINT
)
"""

create_monthly_likes = """
CREATE TABLE IF NOT EXISTS monthly_likes(
    month_year DATE NOT NULL,
    monthly_like_count SMALLINT
)
"""

create_song_details = """
CREATE TABLE IF NOT EXISTS song_details(
    track_name VARCHAR(44) NOT NULL,
    artist_name VARCHAR(255),
    album_name VARCHAR(255),
    duration_ms BIGINT,
    artists_popularity SMALLINT,
    added_at TIMESTAMP
)
"""


# Recent plays analysis

create_recent_tracks_by_popularity =  """
CREATE TABLE IF NOT EXISTS recent_tracks_by_popularity(
    track_name VARCHAR(255) NOT NULL,
    artist_name VARCHAR(44) NOT NULL,
    popularity SMALLINT
)
"""
create_daily_plays =  """
CREATE TABLE IF NOT EXISTS daily_plays(
    date DATE NOT NULL,
    play_count SMALLINT
)
"""

create_recent_play_genre_analysis = """
CREATE TABLE IF NOT EXISTS recent_plays_genre_analysis(
    genres VARCHAR(22) NOT NULL,
    count SMALLINT
)
"""

create_recent_play_summary = """
CREATE TABLE IF NOT EXISTS recent_plays_summary(
    metric VARCHAR(22) NOT NULL,
    value DOUBLE
)
"""

create_recent_play_top_artists = """
CREATE TABLE IF NOT EXISTS recent_plays_top_artists(
    artist_name VARCHAR(44) NOT NULL,
    play_count SMALLINT,
    artist_popularity SMALLINT
)
"""