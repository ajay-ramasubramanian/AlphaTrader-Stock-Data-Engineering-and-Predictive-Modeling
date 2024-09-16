from utils import MinioRetriever, MinioUploader
import pandas as pd
from pyspark.sql import SparkSession


class ProcessRecentPlays():

    # Class variables
    PROCESSED: str = 'processed'
    PRESENTATION: str = 'presentation'

    def __init__(self, user, topic) -> None:
        # Instance variables
        self.user = user
        self.topic = topic
        self.spark = (
            SparkSession.builder
                .appName("Data Pipeline")
                .config("spark.executor.instances", "1")  # Use 1 executor
                .config("spark.executor.cores", "1")     # Use 1 core
                .getOrCreate()
        )
        self.retriever = MinioRetriever(user=user, topic=topic, container=self.PROCESSED)
        self.uploader = MinioUploader(user=user, topic=topic, container=self.PRESENTATION)

    def get_user_followed_artists(self):
        artists = []
        df = self.retriever.retrieve_object()
        df = self.spark.createDataFrame(df)
        df.show()


        # Convert to DataFrame
        # df_artists = pd.DataFrame(artists)
        # self.upload_files(data=df_artists)
        # print("done")

        self.spark.stop()
    
    
if __name__ == '__main__':
    ob = ProcessRecentPlays('suhaas', 'spotify-recent-plays')
    ob.get_user_followed_artists()
