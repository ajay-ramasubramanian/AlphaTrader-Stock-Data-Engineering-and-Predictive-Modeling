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


# def load_schema(schema_path):
#     with open(schema_path, "rb") as schema_file:
#         return parse(schema_file.read())

def load_schema(schema_name):
    # Get the absolute path of the current file (utils.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the full path to the schema file
    schema_path = os.path.join(current_dir, '..', 'schemas', schema_name)
    
    try:
        with open(schema_path, "rb") as schema_file:
            return parse(schema_file.read())
    except FileNotFoundError:
        print(f"Schema file not found: {schema_path}")
        raise
    except Exception as e:
        print(f"Error loading schema: {str(e)}")
        raise


TOPIC_CONFIG = {
    'following_artists': {
        'topic': 'spotify_following_artists',
        'schema': load_schema("following_artists.avsc")
    },
    'liked_songs': {
        'topic': 'spotify_liked_songs',
        'schema': load_schema("liked_songs.avsc")
    },
    'recent_plays': {
        'topic': 'spotify_recent_plays',
        'schema': load_schema("recent_plays.avsc")
    },
    'saved_playlists': {
        'topic': 'spotify_saved_playlists',
        'schema': load_schema("saved_playlists.avsc")
    },
    'top_artists': {
        'topic': 'spotify_top_artists',
        'schema': load_schema("top_artists.avsc")
    },
    'top_songs': {
        'topic': 'spotify_top_songs',
        'schema': load_schema("top_songs.avsc")
    }
}

# Create reverse mapping for easy lookup by topic name
TOPIC_TO_KEY = {v['topic']: k for k, v in TOPIC_CONFIG.items()}