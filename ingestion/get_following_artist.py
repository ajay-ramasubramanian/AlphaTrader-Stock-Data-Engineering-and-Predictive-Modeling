from retrieve_objects import MinioRetriever,MinioUploader
import pandas as pd


class RetrieveFollowingArtists(MinioRetriever,MinioUploader):

    def __init__(self,user, topic,container) -> None:
        MinioRetriever.__init__(self,user, topic)
        MinioUploader.__init__(self,container, user,topic)

    def get_user_followed_artists(self):
        artists = []
        results = MinioRetriever.retrieve_object(self)
        for result in results:
            # Process each artist
            print(f"result: {result}")
            for item in result['artists']['items']:
                artists.append({
                    'name': item['name'],
                    'id': item['id'],
                    'uri': item['uri'],
                    'popularity': item['popularity'],
                    'genres': ', '.join(item['genres']),
                    'followers': item['followers']['total']
                })
        # Convert to DataFrame
        df_artists = pd.DataFrame(artists)
        MinioUploader.upload_files(self,data=df_artists)
        print("object uploaded")
    

if __name__ == "__main__":
    ob = RetrieveFollowingArtists("suhaas","spotify-following-artists","processed")
    ob.get_user_followed_artists()
