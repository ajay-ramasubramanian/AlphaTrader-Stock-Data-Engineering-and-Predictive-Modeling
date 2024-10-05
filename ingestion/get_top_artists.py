import sys,os
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_checks.validate_expectations import validate_expectations
import pandas as pd
from datetime import datetime
from ingestion.retrieve_objects import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG
from dotenv import load_dotenv

load_dotenv()
class RetrieveTopArtists():

    def __init__(self,user, topic,raw, processed) -> None:

        self.retriever = MinioRetriever(user, topic, raw)
        self.uploader = MinioUploader(user, topic, processed)
        self.processed = processed
        self.expectations_suite_name = 'top_artists_suite'

        self.dtype_dict = {
            'artist_name': str,
            'artist_id': str,
            'artist_uri': str,
            'popularity': 'int64',
            'followers': 'int64',
            'image_url': str,
            'spotify_url': str,
            'ingested_on': str
        }

    def get_user_top_artists(self):
        
        try:
            artists = []
            results = self.retriever.retrieve_object()
            for result in results:
                item = result["items"][0]
                artists.append({
                    'artist_id': item['id'],
                    'artist_name': item['name'],
                    'artist_uri': item['uri'],
                    'popularity': item['popularity'],
                    'followers': item['followers']['total'],
                    'genres': ', '.join(item['genres']),
                    'image_url': item['images'][0]['url'] if item['images'] else None,
                    'spotify_url': item['external_urls']['spotify'],
                })

            # Convert to DataFrame
            df_artists = pd.DataFrame(artists)
            df_artists['ingested_on'] = datetime.now().strftime("%Y%m%d%H%M%S")
            
            df_artists = df_artists.astype(self.dtype_dict)
            df_artists.drop_duplicates(['artist_id'], inplace=True)
            df_artists = df_artists.reset_index(drop=True)

            # Run Great Expectations data quality checks
            validate_expectations(df_artists, self.expectations_suite_name)

            self.uploader.upload_files(data=df_artists)
            print(f"Successfully uploaded to '{self.processed}' container!!")

        except Exception as e:
            print(f"Encountered an exception here!!: {e}")
    

def run_retrieve_top_artists():
    ob = RetrieveTopArtists(os.getenv('USER_NAME'), \
                            TOPIC_CONFIG["top_artists"]["topic"],\
                            "raw",\
                            "processed")
    ob.get_user_top_artists()


if __name__ == "__main__":
    run_retrieve_top_artists()


