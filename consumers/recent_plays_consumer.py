from kafka import KafkaConsumer
from base_consumer import BaseKafkaConsumer
from utils import TOPIC_CONFIG
class RecentPlaysConsumer(BaseKafkaConsumer):
    
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
    TOPIC = TOPIC_CONFIG['recent_plays']['topic']

    def __init__(self, group_id):
        super().__init__(RecentPlaysConsumer.TOPIC)
        self.consumer = KafkaConsumer(
            bootstrap_servers = RecentPlaysConsumer.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset = 'earliest',  # Start reading from the earliest message available
            group_id = group_id,  # Assign consumer to a group for offset management
        )
        # Subscribe to the specified topic
        self.consumer.subscribe([RecentPlaysConsumer.TOPIC])

if __name__ == '__main__':
    recent_plays = RecentPlaysConsumer('recent_plays_group')
    recent_plays.consume(recent_plays.consumer)
