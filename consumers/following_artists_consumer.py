from base_consumer import BaseKafkaConsumer

class FollowingArtistsConsumer(BaseKafkaConsumer):
    def __init__(self, group_id):
        super().__init__('spotify_following_artists', group_id)


if __name__ == '__main__':
    top_songs = FollowingArtistsConsumer('following_artists_group')
    top_songs.consume()
