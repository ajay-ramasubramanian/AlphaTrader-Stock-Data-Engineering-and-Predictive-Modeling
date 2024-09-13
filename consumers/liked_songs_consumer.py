from kafka import KafkaConsumer
from base_consumer import BaseKafkaConsumer
from utils import TOPIC_CONFIG
class LikedSongsConsumer(BaseKafkaConsumer):
    
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
    TOPIC = TOPIC_CONFIG['liked_songs']['topic']

    def __init__(self, group_id):
        super().__init__('spotify_liked_songs')
        self.consumer = KafkaConsumer(
            bootstrap_servers = LikedSongsConsumer.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset = 'earliest',  # Start reading from the earliest message available
            group_id = group_id,  # Assign consumer to a group for offset management
        )
        # Subscribe to the specified topic
        self.consumer.subscribe([LikedSongsConsumer.TOPIC])


if __name__ == '__main__':
    liked_songs = LikedSongsConsumer('liked_songs_group')
    liked_songs.consume(liked_songs.consumer)
