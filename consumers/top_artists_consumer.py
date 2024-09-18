from kafka import KafkaConsumer
from .base_consumer import BaseKafkaConsumer
from .utils import TOPIC_CONFIG
class TopArtistsConsumer(BaseKafkaConsumer):
    
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
    TOPIC = TOPIC_CONFIG['top_artists']['topic']

    def __init__(self, group_id):
        super().__init__('spotify_top_artists')
        self.consumer = KafkaConsumer(
            bootstrap_servers = TopArtistsConsumer.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset = 'earliest',  # Start reading from the earliest message available
            group_id = group_id,  # Assign consumer to a group for offset management
        )
        # Subscribe to the specified topic
        self.consumer.subscribe([TopArtistsConsumer.TOPIC])

def run_consumer_top_artists():
    top_artists = TopArtistsConsumer('top_artists_group')
    top_artists.consume(top_artists.consumer)

if __name__ == '__main__':
    run_consumer_top_artists()