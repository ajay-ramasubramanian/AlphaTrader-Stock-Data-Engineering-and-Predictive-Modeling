from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
clientID= os.getenv("SPOTIPY_CLIENT_ID")
clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")
import spotipy
from spotipy.oauth2 import SpotifyOAuth

scope = "user-library-read"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

def get_saved_tracks_as_dataframe():
    tracks = []
    offset = 0
    limit = 50  # Maximum allowed by the API

    while True:
        results = sp.current_user_saved_tracks(limit=limit, offset=offset)
        
        for item in results['items']:
            track = item['track']
            tracks.append({
                'name': track['name'],
                'artist': track['artists'][0]['name'],
                'album': track['album']['name'],
                'release_date': track['album']['release_date'],
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'id': track['id'],
                'uri': track['uri'],
                'added_at': item['added_at']
            })
        
        if len(results['items']) < limit:
            break
        
        offset += limit
        print(f"Retrieved {offset} tracks so far...")

    return pd.DataFrame(tracks)

# Get the DataFrame
users_saved_tracks = get_saved_tracks_as_dataframe()
users_saved_tracks.to_csv('users_saved_tracks.csv', index=False)
