from base_consumer import BaseKafkaConsumer

class RecentPlaysConsumer(BaseKafkaConsumer):
    def __init__(self, group_id):
        super().__init__('spotify_recent_plays', group_id)


if __name__ == '__main__':
    top_songs = RecentPlaysConsumer('recent_plays_group')
    top_songs.consume()
