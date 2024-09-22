import sys,os
import site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG
class RetrieveSavedPlaylist(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,raw, processed) -> None:
        MinioRetriever.__init__(self,user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

    def get_user_saved_playlist(self):
        playlists = []
        results = MinioRetriever.retrieve_object(self)
        # print(results)
        for result in results:
            item = result["items"][0]
            playlists.append({
                'playlist_name': item['name'],
                'playlist_id': item['id'],
                'playlist_uri': item['uri'],
                'owner_name': item['owner']['display_name'],
                'owner_id': item['owner']['id'],
                'is_public': item['public'],
                'is_collaborative': item['collaborative'],
                'total_tracks': item['tracks']['total'],
                'description': item['description']
            })
        # Convert to DataFrame
        df_playlist = pd.DataFrame(playlists)
        df_playlist['ingested_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        MinioUploader.upload_files(self,data=df_playlist)
        print("Object uploaded")
    

def run_retrieve_saved_playlist():
    ob = RetrieveSavedPlaylist("suhaas", \
                            TOPIC_CONFIG["saved_playlists"]["topic"],\
                            "raw", \
                            "processed")
    ob.get_user_saved_playlist()


if __name__ == "__main__":
    run_retrieve_saved_playlist()