import sys,os
import site
import spotipy
from spotipy import SpotifyOAuth
from datetime import datetime
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
import pandas as pd
from transformations.utils import MinioRetriever,MinioUploader, TOPIC_CONFIG
from common_utility_functions.utils import USER_INFO, scope

load_dotenv()

class UserDetailsTables:

    def __init__(self, user, topic, presentation="presentation", host=os.getenv('HOST')) -> None:

        self.uploader = MinioUploader(user, topic, presentation, host)
        self.presentation = presentation

    def upload(self, result):
        self.uploader.upload_files(result)


def processed_to_presentation_user_details():
    user_details =UserDetailsTables("suhaas", \
                            TOPIC_CONFIG["user_details"]['topic'])
    client_id = os.getenv('SPOTIPY_CLIENT_ID')
    client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
    redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI')

    sp= spotipy.Spotify(auth_manager = SpotifyOAuth(client_id=client_id ,
                                                    client_secret=client_secret,
                                                    redirect_uri=redirect_uri,
                                                    scope= scope))
    user = sp.current_user()
    user_dict ={"User ID" : user['id'],
    "Display Name": user['display_name'],
    "Email": user['email'],
    "Country": user['country'],
    "Product": user['product']
    }

    user_details_df = pd.DataFrame(user_dict, index=[0])
    print(user_details_df.to_string())
    user_details.upload(user_details_df)

if __name__ == "__main__":
    processed_to_presentation_user_details()