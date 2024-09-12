import io
import json
import time

import avro.schema
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from avro.io import DatumReader
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from minio.error import S3Error

from producers.utils import TOPIC_CONFIG, TOPIC_TO_KEY

# from spotify_user_data_extraction import users_saved_tracks


def avro_deserializer(avro_bytes, schema):
    reader = DatumReader(schema)
    bytes_reader = io.BytesIO(avro_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    return reader.read(decoder)

def process_dataframe(df, topic_name, partition, offset):

        print("Received DataFrame:")
        print("writing the file to a CSV file")
        df.to_csv(f"{topic_name}_{partition}_{offset}.csv", index = False)
        print("------------------------")


def minio (user, topic, data, offset):
    try:

        # Set up S3 filesystem (MinIO uses S3 protocol)
        fs = s3fs.S3FileSystem(
            endpoint_url="http://localhost:9000",
            key="minioadmin",
            secret="minioadmin"
        )
        # Check if the bucket exists, create it if it doesn't
        if not fs.exists(user):
            fs.mkdir(user)
            print(f"Bucket '{user}' created.")

        # Check if the subfolder exists, create it if it doesn't
        subfolder_path = f"{user}/{topic}"
        if not fs.exists(subfolder_path):
            fs.mkdir(subfolder_path)
            print(f"Subfolder '{topic}' created in bucket '{user}'.")

        obj = json.dumps(data)

        with fs.open(f"{user}/{topic}/{offset}.json", 'w') as f:
            f.write(obj)
        

        print( f"{offset} is successfully uploaded as object {topic}/{offset} to bucket {user}")
    except S3Error as e:
        print(f"error occured: {e}")
    except Exception as e:
        print(f"Error in minio function: {e}")


def consumer(bootstrap_servers=['localhost:9093'],
               group_id='spotify_consumer_group'):
    
    # Create a Kafka consumer
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
    )

    kafka_consumer.subscribe([config['topic'] for config in TOPIC_CONFIG.values()])
    print("Started Consumer")

    temp = []

# Avro deserializer
    try:
        # Try to parse the message as JSON
        while True:
            message = kafka_consumer.poll(timeout_ms=1000)
            if message:
                print(message)
                #TODO: message is a list of records. Right now you are taking only the first one.
                # change the code
                

                record = list(message.values())[0][0]
                topic, user, offset = record.topic, record.key.decode("utf-8"), record.offset
                topic_key = TOPIC_TO_KEY[topic]
                # print(topic, user)
                data = avro_deserializer(record.value, TOPIC_CONFIG[topic_key]['schema'])
                temp.append(data)
                print(f"data: {data}")
                print("------------------------------------------------------------------------------------------------------------------------------------")
                # if len(temp) > 1:
                minio(user, topic, data, offset)
                time.sleep(3)
        
        
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Problematic message: {message.value[:100]}...")  # Print first 100 chars of the message
    except Exception as e:
        print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Consumer stopped")

    finally:
        # Close the consumer
        kafka_consumer.close()

# Run consumer
consumer()