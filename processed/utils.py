import io
import os
import sys
import json
import pyspark
import pandas as pd
import s3fs
from minio import Minio

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class MinioRetriever:
    def __init__(self, user, topic, container) -> None:
        self.container = container #raw
        self.user = user
        self.topic = topic.replace("_","-")

    def retrieve_object(self):
        try:
            # Set up S3 filesystem (MinIO uses S3 protocol)
            fs = s3fs.S3FileSystem(
                endpoint_url="http://localhost:9000",
                key="minioadmin",
                secret="minioadmin"
            )

            # List all objects in the specified subfolder
            paths = [
                self.container,
                f"{self.container}/{self.topic}",
                f"{self.container}/{self.topic}/{self.user}",

                
                f"{self.container}/{self.topic}/{self.user}/{self.container}-{self.topic}.parquet"
            ]
            
            for path in paths:
                print("True" if fs.exists(path) else "False")

            # Construct the path to the parquet file
            parquet_path = f"{self.topic}/{self.user}/{self.container}-{self.topic}.parquet"

            df = self.read_object(parquet_path, self.container)
            
            return df

        except Exception as e:
            print(f"Error in retrieve_and_convert_to_dataframe function: {e}")
            return None
        
    def read_object(self, prefix, bucket):
        try:
            client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False  # Set to True if using HTTPS
            )

            data = client.get_object(bucket, prefix)

            with io.BytesIO(data.read()) as parquet_buffer:
                df = pd.read_parquet(parquet_buffer)

            return df

        except Exception as e:
            print(f"Error in read_object function: {e}")  # More detailed error message
            return None

class MinioUploader:
    def __init__(self, user, topic, container) -> None:
        self.container = container
        self.user = user
        self.topic = topic.replace("_","-")


    def ensure_bucket_exists(self, client, bucket_name):
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully")
        else:
            print(f"Bucket '{bucket_name}' already exists")


    def upload_files(self,data):
            minio_client = Minio(
                "localhost:9000", 
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False  # Keep this False for localhost without HTTPS
            )
            fs = s3fs.S3FileSystem(
                    key="minioadmin",
                    secret="minioadmin",
                    endpoint_url="http://localhost:9000",  # Explicitly set the endpoint URL
                    client_kwargs={'endpoint_url': 'http://localhost:9000'},  
                    use_ssl=False  # Set to False for localhost without HTTPS
                )

            bucket_name = f'{self.container}'
            self.ensure_bucket_exists(minio_client,bucket_name)
            path = f"{self.container}/{self.topic}/{self.user}/{self.container}-{self.topic}.parquet"
            try:
                with fs.open(path, 'wb') as f:
                    data.to_parquet(f, engine='pyarrow', compression='snappy')
            except Exception as e:
                print("\nError occured while uploading file to bucket : {e}")
            
        

TOPIC_CONFIG = {
    'following_artists': {
        'topic': 'spotify_following_artists',
    },
    'liked_songs': {
        'topic': 'spotify_liked_songs',
    },
    'recent_plays': {
        'topic': 'spotify_recent_plays',
    },
    'saved_playlists': {
        'topic': 'spotify_saved_playlists',
        
    },
    'top_artists': {
        'topic': 'spotify_top_artists',
        
    },
    'top_songs': {
        'topic': 'spotify_top_songs',
        
    },
    'related_artists': {
        'topic': 'spotify_related_artists',
        
    },
    'artist_albums': {
        'topic': 'spotify_artist_albums',
        }
}