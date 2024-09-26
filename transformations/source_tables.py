import sys,os
import site
from datetime import datetime

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import pandas as pd
from transformations.utils import MinioRetriever,MinioUploader
from transformations.utils import TOPIC_CONFIG

load_dotenv()

class SourceTables:

    def __init__(self, user, topic, processed="processed", presentation="presentation", host=os.getenv('HOST')) -> None:

        self.retriever = MinioRetriever(user, topic, processed, host)
        self.uploader = MinioUploader(user, topic, presentation, host)
        self.presentation = presentation
        
    def retrieve(self):
        result = self.retriever.retrieve_object()
        return result

    def upload(self, result):
        self.uploader.upload_files(result)


def processed_to_presentation_liked_songs():
    liked_songs = SourceTables("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"]
                            )
    
    results = liked_songs.retrieve()
    liked_songs.upload(results)

def processed_to_presentation_related_artists():
    related_artists = SourceTables("suhaas", \
                            TOPIC_CONFIG["related_artists"]["topic"]
                            )
    
    results = related_artists.retrieve()
    results = results[[col for col in results.columns if col != 'genres']]
    related_artists.upload(results)

def processed_to_presentation_recent_plays():
    recent_plays = SourceTables("suhaas", \
                            TOPIC_CONFIG["recent_plays"]["topic"]
                            )
    
    results = recent_plays.retrieve()
    recent_plays.upload(results)
    

def processed_to_presentation_all_tracks():
    all_tracks = SourceTables("suhaas", \
                            TOPIC_CONFIG["all_tracks"]["topic"]
                            )
    
    results = all_tracks.retrieve()
    all_tracks.upload(results)

def processed_to_presentation_artist_albums():
    artist_albums = SourceTables("suhaas", \
                            TOPIC_CONFIG["artist_albums"]["topic"]
                            )
    
    results = artist_albums.retrieve()
    artist_albums.upload(results)

def processed_to_presentation_genres_table():
    genres_table = SourceTables("suhaas", "spotify_genres_table"
                            # TOPIC_CONFIG["genres_table"]["topic"]
                            )
    
    results = genres_table.retrieve()
    genres_table.upload(results)



if __name__ == "__main__":
    processed_to_presentation_liked_songs()
    processed_to_presentation_related_artists()
    processed_to_presentation_recent_plays()
    processed_to_presentation_all_tracks()
    processed_to_presentation_artist_albums()
    processed_to_presentation_genres_table()