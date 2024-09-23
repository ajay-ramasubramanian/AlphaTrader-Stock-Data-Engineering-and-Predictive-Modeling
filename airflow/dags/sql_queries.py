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

create_tracks_table = """
CREATE TABLE IF NOT EXISTS dim_track (
    track_id VARCHAR(256) PRIMARY KEY,
    track_name VARCHAR(256) NOT NULL,
    duration_ms INTEGER,
    track_popularity INTEGER,
    track_uri VARCHAR(256),
    artist_name VARCHAR(256) NOT NULL,
    album_name VARCHAR(256),
    ingested_on VARCHAR(256)
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



