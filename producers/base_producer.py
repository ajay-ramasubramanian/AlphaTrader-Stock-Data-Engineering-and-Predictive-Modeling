from kafka import KafkaProducer
from kafka.errors import KafkaError
import io,os
import avro.schema
from avro.io import DatumWriter
from concurrent.futures import ThreadPoolExecutor
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from producers.utils import scope, TOPIC_CONFIG
from dotenv import load_dotenv

load_dotenv()
# Kafka broker address
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']

class SpotifyKafkaProducer:
    """
    SpotifyKafkaProducer is a producer class that sends serialized Avro-encoded messages to various Kafka topics.
    It supports multi-threaded message production using a ThreadPoolExecutor.
    """

    def __init__(self):
        """
        Initializes the Kafka producer and sets up a thread pool executor for asynchronous message production.
        """
        # Create a Kafka producer with string key serialization and Gzip compression for message payloads
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=str.encode,
                compression_type='gzip'
            )
        except KafkaError as e:
            print(f"Failed to connect to Kafka: {e}")
        
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

        # Set up a thread pool executor for asynchronous processing with a maximum of 5 worker threads
        self.executor = ThreadPoolExecutor(max_workers=5)  # Adjust based on your concurrency needs

    def avro_serializer(self, data, schema):
        """
        Serializes a Python dictionary into Avro format using the provided schema.

        Args:
            data (dict): The data to be serialized.
            schema (avro.schema.Schema): The Avro schema to use for serialization.

        Returns:
            bytes: The serialized data in Avro format.
    
        """
        try:
            writer = DatumWriter(schema)  # Create an Avro DatumWriter for the provided schema
            bytes_writer = io.BytesIO()  # Create a BytesIO buffer to store serialized data
            encoder = avro.io.BinaryEncoder(bytes_writer)  # Create a BinaryEncoder to write to the buffer
            writer.write(data, encoder)  # Write the data using the writer
            return bytes_writer.getvalue()  # Return the serialized data as bytes
        except Exception as e:
            print(f"schema mismatch: {e}")

    def produce_message(self, topic_key, user_id, data):
        """
        Produces a message to a Kafka topic. The message is serialized into Avro format using the schema defined in `TOPIC_CONFIG`.

        Args:
            topic_key (str): The key to identify the Kafka topic and schema from `TOPIC_CONFIG`.
            user_id (str): The key to be used for partitioning messages in Kafka.
            data (dict): The data payload to be serialized and sent to Kafka.

        Returns:
            Future: A Kafka Future object that can be used to track the result of the send operation.
        """
        # Check if the provided topic_key is valid
        if topic_key not in TOPIC_CONFIG:
            raise ValueError(f"Invalid topic: {topic_key}")
        
        # Retrieve the topic name and schema for the specified topic_key from TOPIC_CONFIG
        topic = TOPIC_CONFIG[topic_key]['topic']
        # print(f"producer: {topic}")
        schema = TOPIC_CONFIG[topic_key]['schema']
        
        # Serialize the data using Avro format
        avro_data = self.avro_serializer(data, schema)
        
        # Send the message to the Kafka topic asynchronously
        future = self.producer.send(topic=topic, key=user_id, value=avro_data)
        return future

    # Methods to produce messages to specific Kafka topics related to Spotify data

    def produce_following_artists(self, user_id, track_data):
        """
        Produces messages related to following artists data for a user.

        Args:
            user_id (str): The user ID.
            track_data (dict): The data payload related to following artists.
        
        Returns:
            Future: A Future object from the ThreadPoolExecutor.
        """
        return self.executor.submit(self.produce_message, 'following_artists', user_id, track_data)

    def produce_liked_songs(self, user_id, album_data):
        """
        Produces messages related to liked songs data for a user.

        Args:
            user_id (str): The user ID.
            album_data (dict): The data payload related to liked songs.
        
        Returns:
            Future: A Future object from the ThreadPoolExecutor.
        """
        return self.executor.submit(self.produce_message, 'liked_songs', user_id, album_data)

    def produce_recent_plays(self, user_id, artist_data):
        """
        Produces messages related to recent plays data for a user.

        Args:
            user_id (str): The user ID.
            artist_data (dict): The data payload related to recent plays.
        
        Returns:
            Future: A Future object from the ThreadPoolExecutor.
        """
        return self.executor.submit(self.produce_message, 'recent_plays', user_id, artist_data)

    def produce_saved_playlists(self, user_id, playlist_data):
        """
        Produces messages related to saved playlists data for a user.

        Args:
            user_id (str): The user ID.
            playlist_data (dict): The data payload related to saved playlists.
        
        Returns:
            Future: A Future object from the ThreadPoolExecutor.
        """
        return self.executor.submit(self.produce_message, 'saved_playlists', user_id, playlist_data)

    def produce_top_artists(self, user_id, profile_data):
        """
        Produces messages related to top artists data for a user.

        Args:
            user_id (str): The user ID.
            profile_data (dict): The data payload related to top artists.
        
        Returns:
            Future: A Future object from the ThreadPoolExecutor.
        """
        return self.executor.submit(self.produce_message, 'top_artists', user_id, profile_data)

    def produce_top_songs(self, user_id, history_data):
        """
        Produces messages related to top songs data for a user.

        Args:
            user_id (str): The user ID.
            history_data (dict): The data payload related to top songs.
        
        Returns:
            Future: A Future object from the ThreadPoolExecutor.
        """
        return self.executor.submit(self.produce_message, 'top_songs', user_id, history_data)

    def close(self):
        """
        Gracefully shuts down the producer and the thread pool executor.
        Ensures that all pending messages are sent and resources are cleaned up.
        """
        self.executor.shutdown()  # Shut down the executor, waiting for all tasks to complete
        self.producer.flush()  # Ensure all buffered records are sent to Kafka
        self.producer.close()  # Close the Kafka producer to release resources
