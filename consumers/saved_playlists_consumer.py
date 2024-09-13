from base_consumer import BaseKafkaConsumer

class SavedPlaylistsConsumer(BaseKafkaConsumer):
    def __init__(self, group_id):
        super().__init__('spotify_saved_playlists', group_id)


if __name__ == '__main__':
    top_songs = SavedPlaylistsConsumer('saved_playlists_group')
    top_songs.consume()
