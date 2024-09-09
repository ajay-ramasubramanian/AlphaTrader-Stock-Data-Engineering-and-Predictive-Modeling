from kafka import KafkaProducer
from utils import schemas, TOPICS
import json
import io
import avro.schema
from avro.io import DatumWriter
from concurrent.futures import ThreadPoolExecutor

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']

class SpotifyKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=str.encode,
            batch_size=16384,
            linger_ms=100,
            compression_type='gzip'
        )
        self.executor = ThreadPoolExecutor(max_workers=5)  # Adjust based on your needs

    def avro_serializer(self, data, schema):
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return bytes_writer.getvalue()

    def produce_message(self, topic_key, user_id, data):
        if topic_key not in TOPICS:
            raise ValueError(f"Invalid topic: {topic_key}")
        
        topic = TOPICS[topic_key]
        schema = schemas[topic_key]
        avro_data = self.avro_serializer(data, schema)
        future = self.producer.send(topic, key=user_id, value=avro_data)
        return future

    def produce_following_artists(self, user_id, track_data):
        return self.executor.submit(self.produce_message, 'following_artists', user_id, track_data)

    def produce_liked_songs(self, user_id, album_data):
        return self.executor.submit(self.produce_message, 'liked_songs', user_id, album_data)

    def produce_recent_plays(self, user_id, artist_data):
        return self.executor.submit(self.produce_message, 'recent_plays', user_id, artist_data)

    def produce_saved_playlists(self, user_id, playlist_data):
        return self.executor.submit(self.produce_message, 'saved_playlists', user_id, playlist_data)

    def produce_top_artists(self, user_id, profile_data):
        return self.executor.submit(self.produce_message, 'top_artists', user_id, profile_data)

    def produce_top_songs(self, user_id, history_data):
        return self.executor.submit(self.produce_message, 'top_songs', user_id, history_data)

    def close(self):
        self.executor.shutdown()
        self.producer.flush()
        self.producer.close()
