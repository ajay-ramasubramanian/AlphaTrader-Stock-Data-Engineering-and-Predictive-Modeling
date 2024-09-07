from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
import spotipy
from dotenv import load_dotenv
from datetime import datetime
import os
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth


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


def get_user_recently_played_tracks():
    tracks = []
    after = None
    limit = 50  # Maximum allowed by the API

    while True:
        results = sp.current_user_recently_played(limit=limit, after=after)
        
        if not results['items']:
            break

        # Process each track
        for item in results['items']:
            track = item['track']
            played_at = datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            
            tracks.append({
                'track_name': track['name'],
                'track_id': track['id'],
                'track_uri': track['uri'],
                'artist_name': track['artists'][0]['name'],
                'artist_id': track['artists'][0]['id'],
                'album_name': track['album']['name'],
                'album_id': track['album']['id'],
                'played_at': played_at,
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity']
            })
        
        # Update the 'after' parameter for the next request
        after = int(played_at.timestamp() * 1000)
        
        print(f"Retrieved {len(tracks)} recently played tracks so far...")

        # Spotify only allows retrieval of the last 50 tracks, so we'll stop after one iteration
        break

    # Convert to DataFrame
    df_tracks = pd.DataFrame(tracks)
    
    # Sort by played_at in descending order (most recent first)
    df_tracks = df_tracks.sort_values('played_at', ascending=False)
    
    return df_tracks


# recently_played_tracks = get_user_recently_played_tracks()

# user_followed_artists = get_user_followed_artists()

users_saved_tracks = get_saved_tracks_as_dataframe()

print("-----DONE------")
