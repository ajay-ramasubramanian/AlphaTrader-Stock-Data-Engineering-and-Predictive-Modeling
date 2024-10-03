# The purpose of this file is to get and set user_name once and use it where ever you want.

# config.py

import os
import spotipy
from spotipy import SpotifyOAuth
from common_utility_functions.utils import scope
USER_NAME =None
def get_user_name():

    client_id = os.getenv('SPOTIPY_CLIENT_ID')
    client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
    redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI')

    sp= spotipy.Spotify(auth_manager = SpotifyOAuth(client_id=client_id ,
                                                    client_secret=client_secret,
                                                    redirect_uri=redirect_uri,
                                                    scope= scope))
            
    user_info= sp.current_user()

    
    os.environ["USER_NAME"]= user_info['display_name']
    
    return user_info['display_name']

def user():
    USER_INFO = get_user_name()


