import json
import s3fs
from minio import Minio

class MinioRetriever:
    def __init__(self, user, topic) -> None:
        self.user = user
        self.topic = topic
    def retrieve_object(self):
        try:
            # Set up S3 filesystem (MinIO uses S3 protocol)
            fs = s3fs.S3FileSystem(
                endpoint_url="http://localhost:9000",
                key="minioadmin",
                secret="minioadmin"
            )

            # List all objects in the specified subfolder
            object_list = fs.ls(f"{self.topic}/{self.user}")
            # Initialize an empty list to store all JSON data
            all_data = []

            # Iterate through each object and read its content
            for obj in object_list:
                with fs.open(obj, 'r') as f:
                    json_data = json.load(f)
                    all_data.append(json_data)
            # print(all_data[0])
            return all_data
            
            # print(f"Successfully retrieved and converted {len(all_data[0])} objects from {self.topic}/{self.user}")
            # # return df

        except Exception as e:
            print(f"Error in retrieve_and_convert_to_dataframe function: {e}")
            return None

class MinioUploader:
    def __init__(self, container, user, topic) -> None:
        self.container = container
        self.user = user
        self.topic = topic


    def ensure_bucket_exists(self,client, bucket_name):
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully")
        else:
            print(f"Bucket '{bucket_name}' already exists")

    def upload_files(self,data):
            minio_client = Minio(
                "localhost:9000",  # Change this to your actual host if different
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False  # Keep this False for localhost without HTTPS
            )
            fs = s3fs.S3FileSystem(
                    key=minio_client.access_key,
                    secret=minio_client.secret_key,
                    endpoint_url="http://localhost:9000",  # Explicitly set the endpoint URL
                    client_kwargs={'endpoint_url': 'http://localhost:9000'},  # Add this line
                    use_ssl=False  # Set to False for localhost without HTTPS
                )

            bucket_name = f'{self.container}'
            self.ensure_bucket_exists(minio_client,bucket_name)
            path = f"{self.container}/{self.topic}/{self.user}/raw_{self.topic}.parquet"
            try:
                with fs.open(path, 'wb') as f:
                    data.to_parquet(f, engine='pyarrow', compression='snappy')
            except Exception as e:
                print("\nError occured while uploading file to bucket : {e}")
            
        
