import avro

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


TOPICS = {
    'following_artists': 'spotify_following_artists',
    'liked_songs': 'spotify_liked_songs',
    'recent_plays': 'spotify_recent_plays',
    'saved_playlists': 'spotify_saved_playlists',
    'top_artists': 'spotify_top_artists',
    'top_songs': 'spotify_top_songs'
}


def load_schema(schema_path):
        with open(schema_path, "rb") as schema_file:
            return avro.schema.parse(schema_file.read())

schemas = {
            'following_artists': load_schema("schemas/following_artists.avsc"),
            'liked_songs': load_schema("schemas/liked_songs.avsc"),
            'recent_plays': load_schema("schemas/recent_plays.avsc"),
            'saved_playlists': load_schema("schemas/saved_playlists.avsc"),
            'top_artists': load_schema("schemas/top_artists.avsc"),
            'top_songs': load_schema("schemas/top_songs.avsc")
        }
