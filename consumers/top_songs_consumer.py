from kafka import KafkaConsumer
from .base_consumer import BaseKafkaConsumer
from .utils import TOPIC_CONFIG

class TopSongsConsumer(BaseKafkaConsumer):

    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
    TOPIC = TOPIC_CONFIG['top_songs']['topic']

    def __init__(self, group_id):
        super().__init__('spotify_top_songs')
        self.consumer = KafkaConsumer(
            bootstrap_servers = TopSongsConsumer.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset = 'earliest',  # Start reading from the earliest message available
            group_id = group_id,  # Assign consumer to a group for offset management
        )
        # Subscribe to the specified topic
        self.consumer.subscribe([TopSongsConsumer.TOPIC])


def run_consumer_top_songs():
    top_songs = TopSongsConsumer('top_songs_group')
    top_songs.consume(top_songs.consumer)

if __name__ == '__main__':
    run_consumer_top_songs()