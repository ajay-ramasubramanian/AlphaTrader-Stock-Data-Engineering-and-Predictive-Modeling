import sys,os
import site
from datetime import datetime
# sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
import pandas as pd
from transformations.utils import MinioRetriever,MinioUploader
from common_utility_functions.utils import  scope, TOPIC_CONFIG


load_dotenv()

class UserDetailsTables:

    def __init__(self, user, topic, presentation="presentation", host=os.getenv('HOST')) -> None:

        self.uploader = MinioUploader(user, topic, presentation, host)
        self.presentation = presentation

    def upload(self, result):
        self.uploader.upload_files(result)

def get_spotify_client():
    import spotipy
    from spotipy import SpotifyOAuth
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    redirect_uri = "http://localhost:8888"
    return spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=scope))
        
def process_user_details():
    sp = get_spotify_client()
    user = sp.current_user()
    user_dict = {
        "User ID": user['id'],
        "Display Name": user['display_name'],
        "Email": user['email'],
        "Country": user['country'],
        "Product": user['product']
    }
    return pd.DataFrame(user_dict, index=[0])


def processed_to_presentation_user_details():

    user_details =UserDetailsTables(os.getenv('USER_NAME'), \
                            TOPIC_CONFIG["user_details"]['topic'],host='localhost')
    user_details_df = process_user_details()

    user_details.upload(user_details_df)

if __name__ == "__main__":
    processed_to_presentation_user_details()