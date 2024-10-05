import sys, os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from transformations.utils import MinioRetriever, MinioUploader
from common_utility_functions.utils import scope, TOPIC_CONFIG

# Load environment variables such as MinIO host and Spotify credentials from .env file.
load_dotenv()

class UserDetailsTables:
    """
    Class to handle uploading user details data to MinIO storage.
    """

    def __init__(self, user, topic, presentation="presentation", host=os.getenv('HOST')) -> None:
        """
        Initialize the uploader with MinIO configurations.
        
        :param user: The username for authentication.
        :param topic: The Kafka topic name related to user details.
        :param presentation: The target folder or category where the data will be uploaded.
        :param host: The host address of the MinIO service.
        """
        self.uploader = MinioUploader(user, topic, presentation, host)
        self.presentation = presentation

    def upload(self, result):
        """
        Upload the user details result (dataframe) to the MinIO storage.
        
        :param result: A DataFrame containing user details.
        """
        self.uploader.upload_files(result)

def get_spotify_client():
    """
    Create a Spotify client authenticated using OAuth and environment variables for credentials.
    Returns the Spotify client instance.
    """
    import spotipy
    from spotipy import SpotifyOAuth
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI')
    
    # Return an authenticated Spotify client.
    return spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                     client_secret=client_secret,
                                                     redirect_uri=redirect_uri,
                                                     scope=scope))

def process_user_details():
    """
    Retrieve current user's details from Spotify and create a pandas DataFrame containing
    user information like ID, display name, email, country, and product type.
    
    :return: A pandas DataFrame with user details.
    """
    sp = get_spotify_client()  # Create an authenticated Spotify client.
    user = sp.current_user()  # Fetch current user's information from Spotify.
    
    # Create a dictionary with relevant user details.
    user_dict = {
        "User ID": user['id'],
        "Display Name": user['display_name'],
        "Email": user['email'],
        "Country": user['country'],
        "Product": user['product']  # Spotify product type (e.g., premium, free).
    }
    
    # Convert the dictionary to a pandas DataFrame.
    return pd.DataFrame(user_dict, index=[0])

def processed_to_presentation_user_details():
    """
    Process user details and upload the data to the MinIO presentation layer.
    The data is uploaded to a pre-configured Kafka topic related to user details.
    """
    # InitializeHere is the continuation of the comments for the second code block:

    # Initialize an instance of UserDetailsTables with MinIO configuration.
    user_details = UserDetailsTables(os.getenv('USER_NAME'), \
                            TOPIC_CONFIG["user_details"]['topic'], host='localhost')
    
    # Process the user details and create a DataFrame.
    user_details_df = process_user_details()

    # Upload the DataFrame to the MinIO presentation layer.
    user_details.upload(user_details_df)

if __name__ == "__main__":
    # Entry point for processing and uploading user details to MinIO.
    processed_to_presentation_user_details()
