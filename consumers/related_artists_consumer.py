from kafka import KafkaConsumer
from base_consumer import BaseKafkaConsumer
from utils import TOPIC_CONFIG

class RelatedArtistsConsumer(BaseKafkaConsumer):
    
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
    TOPIC = TOPIC_CONFIG['related_artists']['topic']

    def __init__(self, group_id):
        super().__init__('spotify_related_artists')
        self.consumer = KafkaConsumer(
            bootstrap_servers = RelatedArtistsConsumer.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset = 'earliest',  # Start reading from the earliest message available
            group_id = group_id,  # Assign consumer to a group for offset management
        )
        # Subscribe to the specified topic
        self.consumer.subscribe([RelatedArtistsConsumer.TOPIC])

if __name__ == '__main__':
    related_artists = RelatedArtistsConsumer('related_artists_group')
    related_artists.consume(related_artists.consumer)
