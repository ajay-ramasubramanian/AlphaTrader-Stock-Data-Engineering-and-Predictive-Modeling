from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
import spotipy
from dotenv import load_dotenv
import os
import pandas as pd

# load_dotenv()
# clientID= os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

scope = "user-library-read user-follow-read"

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

def get_user_followed_artists():
    artists = []
    after = None
    limit = 50  # Maximum allowed by the API

    while True:
        results = sp.current_user_followed_artists(limit=limit, after=after)
        
        # Process each artist
        for item in results['artists']['items']:
            artists.append({
                'name': item['name'],
                'id': item['id'],
                'uri': item['uri'],
                'popularity': item['popularity'],
                'genres': ', '.join(item['genres']),
                'followers': item['followers']['total']
            })
        
        
        if results['artists']['next']:
            after = results['artists']['cursors']['after']
        else:
            break
        
        print(f"Retrieved {len(artists)} artists so far...")

    # Convert to DataFrame
    df_artists = pd.DataFrame(artists)
    
    return df_artists



# Get the DataFrame
user_followed_artists = get_user_followed_artists()
users_saved_tracks = get_saved_tracks_as_dataframe()

user_followed_artists.to_json("user_followed_artists.json", orient="records")
users_saved_tracks.to_json('users_saved_tracks.json', orient="records")
