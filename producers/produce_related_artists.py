import os
import time
import spotipy
from dotenv import load_dotenv

from base_producer import SpotifyKafkaProducer

# Load environment variables such as Spotify API credentials from the .env file.
load_dotenv()

class RelatedArtistsProducer(SpotifyKafkaProducer):
    """
    A Kafka producer class that sends related artists' data to a Kafka topic.
    It uses Spotify's API to fetch related artists for a user's followed artists and streams this data.
    """

    def __init__(self):
        """
        Initialize the RelatedArtistsProducer with a Spotify client and Kafka producer.
        """
        super().__init__()

    def get_artist_ids(self, user_id, artist_ids):
        """
        This method is responsible for initiating the data processing for a given user
        by passing the artist IDs to process Spotify data.
        
        :param user_id: The ID of the Spotify user.
        :param artist_ids: A list of artist IDs that the user follows or has saved.
        """
        self.process_spotify_data(user_id, artist_ids)

    def get_related_artists(self, artist_id):
        """
        Fetches related artists for a given artist using Spotify's API. The related artists
        are those that Spotify considers to be similar based on user listening patterns.

        :param artist_id: The Spotify ID of the artist to find related artists for.
        :return: A list of related artists.
        """
        try:
            # Retrieve related artists from Spotify.
            related = self.sp.artist_related_artists(artist_id)
            time.sleep(0.2)  # Add a small delay to avoid hitting rate limits.
            return related['artists']
        except spotipy.SpotifyException as e:
            # Handle any errors encountered while fetching related artists.
            print(f"Error getting related artists for {artist_id}: {e}")
            return []

    def send_to_kafka(self, user_id, artist_data):
        """
        Sends artist-related data to a Kafka topic. It maps relevant artist information such as
        ID, name, followers, genres, and popularity into a dictionary and sends it to Kafka.

        :param user_id: The ID of the Spotify user.
        :param artist_data: A tuple containing artist details.
        :return: A Kafka future object representing the asynchronous send operation.
        """
        # Map artist data to a dictionary structure for Kafka.
        result = dict(zip(['id', 'name', 'followers', 'genres', 'popularity', 'image', 'type', 'uri'],
                          artist_data))
        result['genres'] = list(result['genres'])  # Ensure genres are a list.
        
        # Produce the artist-related data to Kafka.
        future = self.produce_related_artists(user_id, result)
        print(f"Sent record to Kafka: {result['name']}")
        return future

    def process_spotify_data(self, user_id, artist_ids=None, depth=2, max_artists=1000):
        """
        Processes Spotify data by retrieving related artists for each artist followed by the user.
        The related artists are sent to a Kafka topic. It works recursively up to a specified depth
        to fetch related artists for a specified number of iterations.

        :param user_id: The Spotify user ID.
        :param artist_ids: A set of artist IDs followed by the user.
        :param depth: The number of levels of related artists to traverse (default is 2).
        :param max_artists: The maximum number of related artists to process (default is 1000).
        """
        futures = []  # Stores future objects for asynchronous Kafka sends.
        
        try:
            print("Sending data to Kafka\n")
            artist_set = set()  # Keep track of processed artists.
            to_process = set(artist_ids)  # Artists to process in the current depth level.
            processed = set()  # Keep track of already processed artists.
            c = 0  # Counter for processed artists.

            # Traverse the related artist tree up to the specified depth.
            for _ in range(depth):
                current_level = set()  # Track artists to process at the next level.
                
                for artist_id in to_process:
                    if artist_id not in processed:
                        # Fetch related artists for the current artist.
                        related = self.get_related_artists(artist_id)
                        
                        for artist in related:
                            if len(artist_set) < max_artists:
                                c += 1
                                # Collect the necessary artist details.
                                id = artist['id']
                                name = artist['name']
                                number_of_followers = artist['followers']['total']
                                genres = tuple(artist['genres'])
                                popularity = artist['popularity']
                                image = artist['images'][0]['url'] if artist['images'] else ''
                                type = artist['type']
                                uri = artist['uri']
                                
                                artist_data = (id, name, number_of_followers, genres, popularity, image, type, uri)
                                
                                # Add artist data to the set and send to Kafka if not already processed.
                                if artist_data not in artist_set:
                                    artist_set.add(artist_data)
                                    future = self.send_to_kafka(user_id, artist_data)
                                    futures.append(future)
                                
                                current_level.add(artist['id'])  # Add to next-level processing.
                        
                        processed.add(artist_id)  # Mark the artist as processed.
                    if len(artist_set) >= max_artists:
                        break
                    time.sleep(0.1)  # Add a slight delay to avoid hitting API rate limits.
                
                to_process = current_level  # Move to the next level of artists to process.
                if len(artist_set) >= max_artists:
                    break

            print(f"Total artists processed: {c}")
            print(f"Unique artist records sent: {len(artist_set)}")

            # Handle the results of Kafka sends (wait for all messages to be sent).
            for future in futures:
                try:
                    kafka_future = future.result()  # Wait for Kafka send to complete.
                    record_metadata = kafka_future.get(timeout=10)  # Retrieve Kafka metadata.
                    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                except Exception as e:
                    print(f"Failed to send message: {e}")

        except Exception as e:
            print(f"An Exception occurred: {e}")

        finally:
            # Ensure the Kafka producer is properly closed after processing.
            self.close()

if __name__ == "__main__":
    # Start the Kafka producer and begin processing related artists for the user.
    related_artists_producer = RelatedArtistsProducer()
    related_artists_producer.process_spotify_data(os.getenv('USER_NAME'))
