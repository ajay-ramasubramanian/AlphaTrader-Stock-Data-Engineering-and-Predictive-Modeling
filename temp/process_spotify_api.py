import os
from datetime import datetime
import time
import pandas as pd
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

# load_dotenv()
# clientID= os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

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

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))


def get_saved_tracks_as_dataframe():
    tracks = []
    offset = 0
    limit = 1 #50  # Maximum allowed by the API

    while True:
        results = sp.current_user_saved_tracks(limit=limit, offset=offset)
        tracks.append(results)
 
        if len(results['items']) < limit:
            break
        
        offset += limit
        print(f"Retrieved {offset} tracks so far...")
    print(tracks[-1])

    return tracks

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


def get_user_playlists():
    playlists = []
    offset = 0
    limit = 50

    while True:
        results = sp.current_user_playlists(limit=limit, offset=offset)
        
        if not results['items']:
            break

        # Process each playlist
        for item in results['items']:
            playlists.append({
                'playlist_name': item['name'],
                'playlist_id': item['id'],
                'playlist_uri': item['uri'],
                'owner_name': item['owner']['display_name'],
                'owner_id': item['owner']['id'],
                'is_public': item['public'],
                'is_collaborative': item['collaborative'],
                'total_tracks': item['tracks']['total'],
                'description': item['description'],
            #     'snapshot_id': item['snapshot_id'],
            #     'created_at': datetime.strptime(item['snapshot_id'].split(':')[0], "%Y%m%dT%H%M%S"),
            })
        
        # Update the offset for the next request
        offset += limit
        
        print(f"Retrieved {len(playlists)} playlists so far...")

        # Check if we've retrieved all playlists
        if len(results['items']) < limit:
            break

    # Convert to DataFrame
    df_playlists = pd.DataFrame(playlists)
    
    # # Sort by created_at in descending order (most recent first)
    # df_playlists = df_playlists.sort_values('created_at', ascending=False)
    
    return df_playlists

def get_user_top_artists(time_range='medium_term'):
    artists = []
    offset = 0
    limit = 50

    while True:
        results = sp.current_user_top_artists(time_range=time_range, limit=limit, offset=offset)
        
        if not results['items']:
            break

        # Process each artist
        for item in results['items']:
            artists.append({
                'artist_name': item['name'],
                'artist_id': item['id'],
                'artist_uri': item['uri'],
                'popularity': item['popularity'],
                'followers': item['followers']['total'],
                'genres': ', '.join(item['genres']),
                'image_url': item['images'][0]['url'] if item['images'] else None,
                'spotify_url': item['external_urls']['spotify'],
            })
        
        # Update the offset for the next request
        offset += limit
        
        print(f"Retrieved {len(artists)} top artists so far...")

        # Check if we've retrieved all artists
        if len(results['items']) < limit:
            break

    # Convert to DataFrame
    df_artists = pd.DataFrame(artists)
    
    # Add a rank column based on the order of retrieval
    df_artists['rank'] = range(1, len(df_artists) + 1)
    
    # Sort by rank
    df_artists = df_artists.sort_values('rank')
    
    return df_artists

def get_user_top_tracks(time_range='medium_term'):
# TODO: for large files, chunk the dataset and send it through kafka

    tracks = []
    offset = 0
    limit = 50

    while True:
        results = sp.current_user_top_tracks(time_range=time_range, limit=limit, offset=offset)
        
        if not results['items']:
            break

        # Process each track
        for item in results['items']:
            tracks.append({
                'track_name': item['name'],
                'track_id': item['id'],
                'track_uri': item['uri'],
                'artist_name': item['artists'][0]['name'],
                'artist_id': item['artists'][0]['id'],
                'album_name': item['album']['name'],
                'album_id': item['album']['id'],
                'album_release_date': item['album']['release_date'],
                'duration_ms': item['duration_ms'],
                'popularity': item['popularity'],
                'explicit': item['explicit'],
                'preview_url': item['preview_url'],
                'external_url': item['external_urls']['spotify'],
            })
        
        # Update the offset for the next request
        offset += limit
        
        print(f"Retrieved {len(tracks)} top tracks so far...")

        # Check if we've retrieved all tracks
        if len(results['items']) < limit:
            break

    # Convert to DataFrame
    df_tracks = pd.DataFrame(tracks)
    
    # Add a rank column based on the order of retrieval
    df_tracks['rank'] = range(1, len(df_tracks) + 1)
    
    # Sort by rank
    df_tracks = df_tracks.sort_values('rank')
    
    return df_tracks


# user_top_tracks = get_user_top_tracks()

# user_top_artists = get_user_top_artists()

# user_playlist = get_user_playlists()

# recently_played_tracks = get_user_recently_played_tracks()

# user_followed_artists = get_user_followed_artists()

# users_saved_tracks = get_saved_tracks_as_dataframe()


# sp.current_user_recently_played(limit=50, after=None)
