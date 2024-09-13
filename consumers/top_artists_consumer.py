from base_consumer import BaseKafkaConsumer

class TopArtistsConsumer(BaseKafkaConsumer):
    def __init__(self, group_id):
        super().__init__('spotify_top_artists', group_id)


if __name__ == '__main__':
    top_songs = TopArtistsConsumer('top_artists_group')
    top_songs.consume()
