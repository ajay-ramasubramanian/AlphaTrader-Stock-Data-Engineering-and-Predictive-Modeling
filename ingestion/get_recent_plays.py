import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
from datetime import datetime
import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG

class RetrieveRecentPlays():

    def __init__(self, user, topic, raw, processed) -> None:

        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, topic, processed)
        self.processed = processed
        self.expectations_suite_name = 'recent_plays_suite'

        self.dtype_dict = {
            'recents_id': 'int64',
            'track_name': str,
            'track_id': str,
            'track_uri': str,
            'artist_name': str,
            'artist_id': str,
            'album_name': str,
            'album_id': str,
            'played_at': object,
            'duration_ms': 'int64',
            'popularity': 'int64',
            'ingested_on': str
        }

    def get_user_recent_plays(self):

        try:
            tracks = []
            results = self.retriever.retrieve_object()
            for result in results:
                for count, item in enumerate(result["items"]):
                    track = item['track']
                    tracks.append({
                        'recents_id': count+1,
                        'track_id': track['id'],
                        'track_name': track['name'],
                        'track_uri': track['uri'],
                        'artist_name': track['artists'][0]['name'],
                        'artist_id': track['artists'][0]['id'],
                        'album_name': track['album']['name'],
                        'album_id': track['album']['id'],
                        'played_at': item['played_at'],
                        'duration_ms': track['duration_ms'],
                        'popularity': track['popularity']
                    })

            # Convert to DataFrame
            df_recent_plays = pd.DataFrame(tracks)
            # df_recent_plays['played_at'] = pd.to_datetime(df_recent_plays['played_at'])\
            #     .apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').astimezone(pytz.UTC))
            df_recent_plays['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")
            
            df_recent_plays = df_recent_plays.astype(self.dtype_dict)
            df_recent_plays.drop_duplicates(['played_at'], inplace=True)
            df_recent_plays = df_recent_plays.reset_index(drop=True)

            # Run Great Expectations data quality checks
            validate_expectations(df_recent_plays, self.expectations_suite_name)

            self.uploader.upload_files(data=df_recent_plays)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")

    
def run_retrieve_recent_plays():
    ob = RetrieveRecentPlays("suhaas", \
                            TOPIC_CONFIG["recent_plays"]["topic"], \
                            "raw", \
                            "processed")
    ob.get_user_recent_plays()


if __name__ == "__main__":
    run_retrieve_recent_plays()
    