from base_consumer import BaseKafkaConsumer

class LikedSongsConsumer(BaseKafkaConsumer):
    def __init__(self, group_id):
        super().__init__('spotify_liked_songs', group_id)


if __name__ == '__main__':
    top_songs = LikedSongsConsumer('liked_songs_group')
    top_songs.consume()
