import sys,os
import site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG

class RetrieveTopSongs(MinioRetriever,MinioUploader):

    def __init__(self,user, topic, raw, processed) -> None:
        MinioRetriever.__init__(self,user, topic, raw)
        MinioUploader.__init__(self, user, topic, processed)

    def get_user_top_songs(self):
        try:
            tracks = []
            results = MinioRetriever.retrieve_object(self)
            for result in results:
                # print(f'result : {result}')
                item = result["items"][0]
                # print(item)
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
                    'external_url': item['external_urls']['spotify'],
                })
            # Convert to DataFrame
            df_artists = pd.DataFrame(tracks)
            print(df_artists.head(20))
            MinioUploader.upload_files(self,data=df_artists)
        # return df_artists
        except Exception as e:
            print(f" Error has occured  : {e}")


def run_retrieve_top_songs():
    obj= RetrieveTopSongs("suhaas", \
                    TOPIC_CONFIG["top_songs"]["topic"], \
                    "raw", \
                    "processed")

    obj.get_user_top_songs()


if __name__ == "__main__":
    run_retrieve_top_songs()
