from kafka import KafkaConsumer
from utils import TOPIC_CONFIG, TOPIC_TO_KEY
import json
import io
import time
from collections import defaultdict
import s3fs
from minio.error import S3Error
import minio
import avro.schema
from avro.io import DatumReader
from concurrent.futures import ThreadPoolExecutor

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']

class BaseKafkaConsumer:

    BATCH_SIZE = 30 # to change
    MAX_BATCH_TIME = 100 # to change
    
    def __init__(self, topic, consumer_group_id):
        self.topic = topic
        self.user_batches = defaultdict(list)
        self.active_users = set()
        self.consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id=consumer_group_id,
        )
        self.consumer.subscribe([topic])

    def avro_deserializer(self, avro_bytes, schema):
        reader = DatumReader(schema)
        bytes_reader = io.BytesIO(avro_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        return reader.read(decoder)
    
    def minio (user, topic, data, offset):
        try:
            ### IMPORTANT: It is not yet batch processing. We are still uploading each object to lake.  
            # Set up S3 filesystem (MinIO uses S3 protocol)
            fs = s3fs.S3FileSystem(
                endpoint_url="http://localhost:9000",
                key="minioadmin",
                secret="minioadmin"
            )

            obj = json.dumps(data)

            with fs.open(f"{user}/{topic}/{offset}.json", 'w') as f:
                f.write(obj)
            

            print( f"{offset} is successfully uploaded as object {topic}/{offset} to bucket {user}")
        except S3Error as e:
            print(f"error occured: {e}")
        except Exception as e:
            print(f"Error in minio function: {e}")

    
    def upload_user_batch(self, user, offset):
            minio(user, self.topic, self.user_batches[user], offset)
            self.user_batches[user] = []


    def process_message(self, message):
        records = list(message.values())[0] # multiple records
        for record in records:
            topic_key, user, offset = TOPIC_TO_KEY[record.topic], record.key.decode("utf-8"), record.offset
            data = self.avro_deserializer(record.value, TOPIC_CONFIG[topic_key]['schema'])
            self.user_batches[user].append(data)
            if len(self.user_batches[user]) >= BaseKafkaConsumer.BATCH_SIZE:
                print(f"Upload data to {self.topic}/{user}")
                self.upload_user_batch(user, offset)
                

    def consume(self, ):
        try: 
            while True:
                print("Polling for messages")
                message = self.consumer.poll(timeout_ms = 3000)
                if message:
                    print(f"Received message")
                    self.process_message(message)
                    self.consumer.commit()
        except KeyboardInterrupt as e:
            print(f"Stopping consumer for topic: {self.topic}")
        finally:
            self.consumer.close()




