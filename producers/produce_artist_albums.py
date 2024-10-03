import os
from spotipy import Spotify
import time
import spotipy
from dotenv import load_dotenv
from base_producer import SpotifyKafkaProducer
from utils import scope

# Load environment variables from .env file (if needed)
# load_dotenv()
# clientID = os.getenv("SPOTIPY_CLIENT_ID")
# clientSecret = os.getenv("SPOTIPY_CLIENT_SECRET")
# redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")

load_dotenv()
class ArtistAlbumsProducer(SpotifyKafkaProducer):
    """
    A producer class for sending a artists related to artists followed by the user to a Kafka topic.
    """

    def __init__(self):
        """
        Initialize the SavedTracksProducer with a Spotify client and Kafka producer.
        """
        super().__init__()


    def get_artist_ids(self, user_id, artist_ids):
        self.process_spotify_data(user_id, artist_ids)


    def get_all_artist_albums(self, sp, artist_id):
        albums = []
        results = sp.artist_albums(artist_id, album_type='album,single,compilation', limit=1)
        albums.append(results['items'])
        while results['next']:
            results = sp.next(results)
            albums.append(results['items'])
        return albums


    def process_spotify_data(self, user_id, artist_ids=None):
        futures = []
        try:
            print("Sending data to Kafka")
            artist_ids = list(dict.fromkeys(artist_ids))
            print(len(artist_ids))
    
            for artist_id in artist_ids:
                print(f"Processing artist: {artist_id}")
                albums = []
                result = self.sp.artist_albums(artist_id, album_type='album,single,compilation', limit=10)
                time.sleep(0.2)
                while result['items'] and len(albums) < 10:
                    albums.extend(result['items'])
                    if len(albums) < 10 and result['next']:
                        result = self.sp.next(result)
                    else:
                        break

                for album in albums:
                    future = self.produce_artist_albums(user_id, album)
                    print(f"Sent record to Kafka: {album['name']}")
                    futures.append(future)

                print(f"Processed {len(albums)} albums for artist {artist_id}")

            print("Sent all the data")

            # Wait for all Kafka messages to be sent and handle their results
            for future in futures:
                try:
                    kafka_future = future.result()
                    record_metadata = kafka_future.get(timeout=10)
                    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                except Exception as e:
                    print(f"Failed to send message: {e}")

        finally:
            self.close()

if __name__ == "__main__":
    artist_ids=['2BNj9F2NMmWTRpO3S3C0dK', '6sCbFbEjbYepqswM1vWjjs', '0VOR7Ie9xUSb45fzIIVJQ1', '4zCH9qm4R2DADamUHMCa6O', '45eNHdiiabvmbp4erw26rg', '1vCWHaC5f2uS3yhpwWbIA6', '04gDigrS5kc9YWfZHwBETP', '69GGBxA162lTqCwzJG5jLp', '3EOEK57CV77D4ovYVcmiyt', '2h93pZq0e7k5yf4dywlkpM', '6sCbFbEjbYepqswM1vWjjs', '1Xyo4u8uXC1ZmMpatF05PJ', '1Xyo4u8uXC1ZmMpatF05PJ', '2LZDXcxJWgsJfKXZv9a5eG', '2LZDXcxJWgsJfKXZv9a5eG', '2LZDXcxJWgsJfKXZv9a5eG', '2LZDXcxJWgsJfKXZv9a5eG', '2LZDXcxJWgsJfKXZv9a5eG', '2LZDXcxJWgsJfKXZv9a5eG', '2LZDXcxJWgsJfKXZv9a5eG', '4nVa6XlBFlIkF6msW57PHp', '6Wr3hh341P84m3EI8qdn9O', '02A3cEvlLLCbIMVDrK2GHV', '66CXWjxzNUsdJxJ2JdwvnR', '69GGBxA162lTqCwzJG5jLp', '6deZN1bslXzeGvOLaLMOIF', '6XyY86QOPPrYVGvF9ch6wz', '5BtHciL0e0zOP7prIHn3pP', '4CTKqs11Zgsv8EZTVzx764', '4CTKqs11Zgsv8EZTVzx764', '4CTKqs11Zgsv8EZTVzx764', '5hREZP0zTQbTLkZ2M8RS4v', '20JMfmzDb5cjHxEoMXXMyY', '7m0BsF0t3K9WQFgKoPejfk', '6XyY86QOPPrYVGvF9ch6wz', '6B5c4sch27tWHAGdarpPaW', '2sil8z5kiy4r76CRTXxBCA', '1Ffb6ejR6Fe5IamqA5oRUF', '3IYUhFvPQItj6xySrBmZkd', '3Nrfpe0tUJi4K4DXYWgMUX', '3Nrfpe0tUJi4K4DXYWgMUX', '6HaGTQPmzraVmaVxvz6EUc', '6HaGTQPmzraVmaVxvz6EUc', '6HaGTQPmzraVmaVxvz6EUc', '3JsHnjpbhX4SnySpvpa9DK', '6DIS6PRrLS3wbnZsf7vYic', '6VxCmtR7S3yz4vnzsJqhSV', '5Pb27ujIyYb33zBqVysBkj', '7DMveApC7UnC2NPfPvlHSU', '21mKp7DqtSNHhCAU2ugvUw', '6vXTefBL93Dj5IqAWq6OTv', '3p5nfUyEYsxx8izMCym07n', '0Ye4nfYAA91T1X56gnlXAA', '4AA8eXtzqh5ykxtafLaPOi', '2MqhkhX4npxDZ62ObR5ELO', '1McMsnEElThX1knmY4oliG', '08GQAI4eElDnROBrJRGE0X', '1LOB7jTeEV14pHai6EXSzF', '2P5sC9cVZDToPxyomzF1UH', '2P5sC9cVZDToPxyomzF1UH', '2LZDXcxJWgsJfKXZv9a5eG', '5oyy3Tb7hqV3BC8eiqEoU0', '0Y5tJX1MQlPlqiwlOH1tJY', '2sf28o6euxEDpYkG9dMtuM', '1Xyo4u8uXC1ZmMpatF05PJ', '1hCkSJcXREhrodeIHQdav8', '1hCkSJcXREhrodeIHQdav8', '4IuWSIEfIed8aZb49YA4Cj', '47mIJdHORyRerp4os813jD', '6USMTwO0MNDnKte5a5h0xx', '3MZsBdqDrRTJihTHQrO6Dq', '4BzCdhJTyTS3gumq9xmymb', '5Pwc4xIPtQLFEnJriah9YJ', '0jNDKefhfSbLR9sFvcPLHo', '5i4BaWy8YIun1u3RXwmlWn', '5JZ7CnR6gTvEMKX4g70Amv', '5IH6FPUwQTxPSXurCrcIov', '26VFTg2z8YR0cCuwLzESi2', '6Cny0Wt5OKLct1rGOLmu80', '0yb46jwm7gqbZXVXZQ8Z1e', '2cnMpRsOVqtPMfq7YiFE6K', '0xe3AMjZeR6z3g4O6Vppjq', '33Cf4O1KAVbtQa00scMi2A', '2MGL4XU2LCJC47c7VvSwuE', '1Gt39QnTBTndeyiJ5SO04q', '4AlA8vTiQdHHCKVBMJKHBg', '2LuHL7im4aCEmfOlD4rxBC', '4qIVPF0s71ZYW3qzhu5GkF', '2ODUxmFxJSyvGiimNhMHbO', '78FpcvFFNC1ZTrUvWb0tlm', '4NpFxQe2UvRCAjto3JqlSl', '3bLSAQPeix7Xm2e5Gtn48R', '62G1LvBKkkl7sLHqrGbAcp', '7jdFEYD2LTYjfwxOdlVjmc', '7AhDVqsNA5q46WKsRPXvoe', '3VSHFjwG94ubNcoST9hfxX', '4MCBfE4596Uoi2O4DtmEMz', '7wg1qvie3KqDNQbAkTdbX0', '4aCkc4HrwC4Dopig7RELoH', '1xKrH6GSh9CJh8nYwbqW7B', '4gzpq5DPGxSnKTe4SA8HAU', '4Mqr7ZLQ6xzHLvAQ1YnwCQ', '6cadOIa5DTh6a5mGo5r4bh', '6cadOIa5DTh6a5mGo5r4bh', '68kEuyFKyqrdQQLLsmiatm', '4AA8eXtzqh5ykxtafLaPOi', '45eNHdiiabvmbp4erw26rg', '5ppJZFMF0gAfrHfZTGuHe4', '3dkie6yywJJVPCTO4b5AP9', '20DZAfCuP1TKZl5KcY7z3Q', '7lZauDnRoAC3kmaYae2opv', '6olWbKW2VLhFCHfOi0iEDb', '6olWbKW2VLhFCHfOi0iEDb', '4Do9oNUGTHFkYbrTvOBfCk', '7C3Cbtr2PkH2l4tOGhtCsk', '7blXVKBSxdFZsIqlhdViKc', '0mUIey7n1U90IScto35rX7', '7wg1qvie3KqDNQbAkTdbX0', '6LuN9FCkKOj5PcnpouEgny', '0ZjUUrNDmi4N4Ey5UTMebc', '0ZjUUrNDmi4N4Ey5UTMebc', '24V5UY0nChKpnb1TBPJhCw', '7vGkwxpdjmp2UoWRIfqrVc', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '6LuN9FCkKOj5PcnpouEgny', '1si6mnxJ6IpTOTW13ECa0o', '6gK1Uct5FEdaUWRWpU4Cl2', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '45eNHdiiabvmbp4erw26rg', '2W6kbe0nm96COrHzNmfLLd', '7jVv8c5Fj3E9VhNjxT4snq', '0D1FNjQMAVognp7FFrjGte', '1IueXOQyABrMOprrzwQJWN', '3v6Ji4uoWtKRkhuDUaxi9n', '60d24wfXkVzDSfLS6hyCjZ', '23fqKkggKUBHNkbKtXEls4', '2zKbqRqx22axcZA2mUw71Y', '6DPYiyq5kWVQS4RGwxzPC7', '711MCceyCBcFnzjGY4Q7Un', '711MCceyCBcFnzjGY4Q7Un', '711MCceyCBcFnzjGY4Q7Un', '7ewJCECkpkh56w0Y4VWzSY', '3hv9jJF3adDNsBSIQDqcjp', '7LIy7KinYq7a83dUH6kvxT', '4zjO8Jhi2pciJJzd8Q6rga', '6g878dtAhjegRHVe5X0ALf', '5Pb27ujIyYb33zBqVysBkj', '2NdeV5rLm47xAvogXrYhJX', '58MLl9nC29IXbE4nEtuoP2', '1l8Fu6IkuTP0U5QetQJ5Xt', '246dkjvS1zLTtiykXe5h60', '2GH5uRMxmuAWn90B8DEZU3', '4lDXfIznmGueBgTjI3qGUX', '6mA4csYsYvf4Mq02PleZEV', '6mA4csYsYvf4Mq02PleZEV', '60ELFqAhwT7jwRKJV6Rgfx', '6eBIpic8O1RW435sVsjYfy', '3HqP3nd8WI0VfHRhApPlan', '4lDXfIznmGueBgTjI3qGUX', '3HqP3nd8WI0VfHRhApPlan', '4eFImh8D3F15dtZk0JQlpT', '6mA4csYsYvf4Mq02PleZEV', '3HqP3nd8WI0VfHRhApPlan', '3AVfmawzu83sp94QW7CEGm', '5jAMCwdNHWr7JThxtMuEyy', '4m1yRHUMhvB8gKAJTjK4kO', '1QRj3hoop9Mv5VvHQkwPEp', '60d24wfXkVzDSfLS6hyCjZ', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '5ppJZFMF0gAfrHfZTGuHe4', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '2q3GG88dVwuQPF4FmySr9I', '22lnnGKlaDxk8sfzCNRJuA', '0VOR7Ie9xUSb45fzIIVJQ1', '4LZ4De2MoO3lP6QaNCfvcu', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '2NhdGz9EDv2FeUw6udu2g1', '3NPpFNZtSTHheNBaWC82rB', '1R2iRWvuwwokMKGHirNGTD', '46SHBwWsqBkxI7EeeBEQG7', '23fqKkggKUBHNkbKtXEls4', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '7oPftvlwr6VrsViSDV7fJY', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '04gDigrS5kc9YWfZHwBETP', '5F1aoppMtU3OMiltO8ymJ2', '2cQr2KbzdRtIFlfbHGnNsL', '6Y20PijIkUoCtuvtkz37dO', '5F1aoppMtU3OMiltO8ymJ2', '5F1aoppMtU3OMiltO8ymJ2', '0LyOADBjj28cbvJWTXUEGA', '7HV2RI2qNug4EcQqLbCAKS', '6U1dBXJhC8gXFjamvFTmHg', '2LAqcqAQ8KPTsl1HBgBrqM', '0LyOADBjj28cbvJWTXUEGA', '5F1aoppMtU3OMiltO8ymJ2', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '15UsOTVnJzReFVN1VCnxy4', '3QSQFmccmX81fWCUSPTS7y', '5X4LWwbUFNzPkEas04uU82', '5Pwc4xIPtQLFEnJriah9YJ', '5Pwc4xIPtQLFEnJriah9YJ', '5iNrZmtVMtYev5M9yoWpEq', '1Xylc3o4UrD53lo9CvFvVg', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '24eDfi2MSYo3A87hCcgpIL', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '53XhwfbYqKCa1cC15pYq2q', '24eDfi2MSYo3A87hCcgpIL', '1BhWF9W2PngtPSyobKg0rP', '2LZDXcxJWgsJfKXZv9a5eG', '6VuMaDnrHyPL1p4EHjYLi7', '04gDigrS5kc9YWfZHwBETP', '5ZsFI1h6hIdQRw2ti0hz81', '4xRYI6VqpkE3UwrDrAZL8L', '31TPClRtHm23RisEBtV3X7', '7DMveApC7UnC2NPfPvlHSU', '2Z9xsgKEjZF8ueZ96rV6nH', '0du5cEVh5yTK9QJze8zA0C', '0du5cEVh5yTK9QJze8zA0C', '0du5cEVh5yTK9QJze8zA0C', '0du5cEVh5yTK9QJze8zA0C', '0du5cEVh5yTK9QJze8zA0C', '0du5cEVh5yTK9QJze8zA0C', '0du5cEVh5yTK9QJze8zA0C', '0du5cEVh5yTK9QJze8zA0C', '2WX2uTcsvV5OnS0inACecP', '6vWDO969PvNqNYHIOW5v0m', '3ScY9CQxNLQei8Umvpx5g6', '6nxWCVXbOlEVRexSbLsTer', '6XyY86QOPPrYVGvF9ch6wz', '0YC192cP3KPCRWx8zr8MfZ', '5pUo3fmmHT8bhCyHE52hA6', '5pKCCKE2ajJHZ9KAiaK11H', '07YZf4WDAMNwqr4jfgOZ8y', '4AVFqumd2ogHFlRbKIjp1t', '6eUKZXaKkcviH0Ku9w2n3V', '60d24wfXkVzDSfLS6hyCjZ', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '69GGBxA162lTqCwzJG5jLp', '4qBgvVog0wzW75IQ48mU7v', '0du5cEVh5yTK9QJze8zA0C', '69GGBxA162lTqCwzJG5jLp', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V', '3AQRLZ9PuTAozP28Skbq8V']
    # Start the data processing for a specific user
    artist_albums_producer = ArtistAlbumsProducer()
    artist_albums_producer.process_spotify_data(os.getenv('USER_NAME'), artist_ids=artist_ids)
