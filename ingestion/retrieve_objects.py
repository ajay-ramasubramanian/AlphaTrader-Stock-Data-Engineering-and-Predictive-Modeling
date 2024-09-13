import s3fs
import json
import pandas as pd
from io import StringIO

def retrieve_and_convert_to_dataframe(user, topic):
    try:
        # Set up S3 filesystem (MinIO uses S3 protocol)
        fs = s3fs.S3FileSystem(
            endpoint_url="http://localhost:9000",
            key="minioadmin",
            secret="minioadmin"
        )

        # List all objects in the specified subfolder
        object_list = fs.ls(f"{topic}/{user}")

        # Initialize an empty list to store all JSON data
        all_data = []

        # Iterate through each object and read its content
        for obj in object_list:
            with fs.open(obj, 'r') as f:
                json_data = json.load(f)
                for record in json_data: # unpacking batched data if any
                    print(f"record: {record}")
                    all_data.append(record)
        print(len(all_data))
        # print(all_data)


        
        print(f"Successfully retrieved and converted {len(all_data)} objects from {user}/{topic}")
        # return df

    except Exception as e:
        print(f"Error in retrieve_and_convert_to_dataframe function: {e}")
        return None

retrieve_and_convert_to_dataframe("suhaas","spotify-top-songs")