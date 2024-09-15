from retrieve_objects import MinioRetriever, MinioUploader
import pandas as pd

class RetrieveTopSongs(MinioRetriever,MinioUploader):

    def __init__(self,user, topic, container) -> None:
        MinioRetriever.__init__(self,user, topic)
        MinioUploader.__init__(self,container, user, topic)

    def get_user_top_songs(self):
        try:
            tracks = []
            results = super().retrieve_object(self)
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
                    'external_url': item['external_urls']['spotify'],
                })
            # Convert to DataFrame
            df_artists = pd.DataFrame(tracks)
            MinioUploader.upload_files(self,data=df_artists)
        # return df_artists
        except Exception as e:
            print(f" Error has occured  : {e}")


obj= RetrieveTopSongs("suhaas", "spotify-following-artists" ,"processed")

obj.get_user_top_songs()
