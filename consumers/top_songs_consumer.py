from base_consumer import BaseKafkaConsumer

class TopSongsConsumer(BaseKafkaConsumer):
    def __init__(self, group_id):
        super().__init__('spotify_top_songs', group_id)


if __name__ == '__main__':
    top_songs = TopSongsConsumer('top_songs_group')
    top_songs.consume()
