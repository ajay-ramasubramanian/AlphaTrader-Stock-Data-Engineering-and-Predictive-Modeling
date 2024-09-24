import sys,os
import site
from datetime import datetime

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from transformations.utils import MinioRetriever,MinioUploader
from transformations.utils import TOPIC_CONFIG


class SourceTables:

    def __init__(self,user, topic, processed, presentation,host) -> None:

        self.retriever = MinioRetriever(user, topic, processed,host)
        self.uploader = MinioUploader(user,topic, presentation,host)
        self.presentation = presentation
        
    def retrieve_and_upload(self):
        result = self.retriever.retrieve_object()
        self.uploader.upload_files(result)


def run_retrieve_liked_songs():
    liked_songs = SourceTables("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "processed" , \
                            "presentation", \
                            'minio'
                            )
    liked_songs.retrieve_and_upload()

def run_related_artists():
    related_artists = SourceTables("suhaas", \
                            TOPIC_CONFIG["related_artists"]["topic"], \
                            "processed" , \
                            "presentation", \
                            'minio'
                            )
    related_artists.retrieve_and_upload()

def run_recent_plays():
    recent_plays = SourceTables("suhaas", \
                            TOPIC_CONFIG["recent_plays"]["topic"], \
                            "processed" , \
                            "presentation", \
                            'minio'
                            )
    recent_plays.retrieve_and_upload()
    

def run_all_tables():
    all_tracks = SourceTables("suhaas", \
                            TOPIC_CONFIG["all_tracks"]["topic"], \
                            "processed" , \
                            "presentation", \
                            'minio'
                            )
    all_tracks.retrieve_and_upload()

def run_artist_albums():
    artist_albums = SourceTables("suhaas", \
                            TOPIC_CONFIG["artist_albums"]["topic"], \
                            "processed" , \
                            "presentation", \
                            'minio'
                            )
    artist_albums.retrieve_and_upload()



if __name__ == "__main__":
    run_retrieve_liked_songs()
    run_related_artists()
    run_recent_plays()
    run_all_tables()
    run_artist_albums()