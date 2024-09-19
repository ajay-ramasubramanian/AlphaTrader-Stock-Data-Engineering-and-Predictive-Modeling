from kafka import KafkaConsumer
from base_consumer import BaseKafkaConsumer
from utils import TOPIC_CONFIG
class SavedPlaylistsConsumer(BaseKafkaConsumer):
    
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
    TOPIC = TOPIC_CONFIG['saved_playlists']['topic']

    def __init__(self, group_id):
        super().__init__('spotify_saved_playlists')
        self.consumer = KafkaConsumer(
            bootstrap_servers = SavedPlaylistsConsumer.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset = 'earliest',  # Start reading from the earliest message available
            group_id = group_id,  # Assign consumer to a group for offset management
        )
        # Subscribe to the specified topic
        self.consumer.subscribe([SavedPlaylistsConsumer.TOPIC])

def run_consumer_saved_playlist():
    saved_playlists = SavedPlaylistsConsumer('saved_playlists_group')
    saved_playlists.consume(saved_playlists.consumer)

if __name__ == '__main__':
    run_consumer_saved_playlist()