import site
sys.path.extend(site.getsitepackages())
import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'utils')))

from avro.schema import parse

scope = "user-library-read \
         user-follow-read \
         playlist-read-private \
         playlist-read-collaborative \
         user-top-read \
         user-read-recently-played \
         playlist-modify-public \
         playlist-modify-private \
         user-read-private \
         user-read-email"


def load_schema(schema_path):
    with open(schema_path, "rb") as schema_file:
        return parse(schema_file.read())


TOPIC_CONFIG = {
    'following_artists': {
        'topic': 'spotify_following_artists',
        'schema': load_schema("schemas/following_artists.avsc")
    },
    'liked_songs': {
        'topic': 'spotify_liked_songs',
        'schema': load_schema("schemas/liked_songs.avsc")
    },
    'recent_plays': {
        'topic': 'spotify_recent_plays',
        'schema': load_schema("schemas/recent_plays.avsc")
    },
    'saved_playlists': {
        'topic': 'spotify_saved_playlists',
        'schema': load_schema("schemas/saved_playlists.avsc")
    },
    'top_artists': {
        'topic': 'spotify_top_artists',
        'schema': load_schema("schemas/top_artists.avsc")
    },
    'top_songs': {
        'topic': 'spotify_top_songs',
        'schema': load_schema("schemas/top_songs.avsc")
    },
    'related_artists': {
        'topic': 'spotify_related_artists',
        'schema': load_schema("schemas/related_artists.avsc")
    },
    'artist_albums': {
        'topic': 'spotify_artist_albums',
        'schema': load_schema("schemas/artist_albums.avsc")
    }
}

# Create reverse mapping for easy lookup by topic name
TOPIC_TO_KEY = {v['topic']: k for k, v in TOPIC_CONFIG.items()}