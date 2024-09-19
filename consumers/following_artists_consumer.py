import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka import KafkaConsumer
from base_consumer import BaseKafkaConsumer
from utils import TOPIC_CONFIG
class FollowingArtistsConsumer(BaseKafkaConsumer):
    
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
    TOPIC = TOPIC_CONFIG['following_artists']['topic']

    def __init__(self, group_id):
        super().__init__('spotify_following_artists')
        self.consumer = KafkaConsumer(
            bootstrap_servers = FollowingArtistsConsumer.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset = 'earliest',  # Start reading from the earliest message available
            group_id = group_id,  # Assign consumer to a group for offset management
        )
        # Subscribe to the specified topic
        self.consumer.subscribe([FollowingArtistsConsumer.TOPIC])


def run_consumer_following_artists():
    following_artists = FollowingArtistsConsumer('following_artists_group')
    following_artists.consume(following_artists.consumer)

if __name__ == '__main__':
    run_consumer_following_artists()