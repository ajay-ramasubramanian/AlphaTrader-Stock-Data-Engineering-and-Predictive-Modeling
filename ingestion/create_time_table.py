import sys,os
import site
sys.path.extend(site.getsitepackages())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from transformations.utils import MinioRetriever,MinioUploader
from ingestion.utils import TOPIC_CONFIG

class RetrieveLikedSongs(MinioRetriever,MinioUploader):

    def __init__(self,user, topic, processed, presentation) -> None:
        self.retriver = MinioRetriever(user, topic, processed)
        # MinioUploader.__init__(self,user,topic, processed)
    def get_user_liked_songs(self):
        
        date_dict = dict()
        liked_songs = self.retriver.retrieve_object()

        # broken code, have to fix it: works without the unique method
        date_time = liked_songs['added_at'].unique()

        date_dict['date_id'] = [dt.strftime('%Y%m%d%H%M%S') for dt in list(date_time)]
        date_dict['year'] = list(date_time.dt.year)
        date_dict['month'] = list(date_time.dt.month)
        date_dict['day'] = list(date_time.dt.day)
        date_dict['hour'] = list(date_time.dt.hour)
        date_dict['minute'] = list(date_time.dt.minute)
        date_dict['second'] = list(date_time.dt.second)

        
        date_time_df = pd.DataFrame(date_dict)
        print(date_time_df.dtypes)
        print(date_time_df)


def run_retrieve_liked_songs():
    ob = RetrieveLikedSongs("suhaas", \
                            TOPIC_CONFIG["liked_songs"]["topic"], \
                            "processed", \
                            "presentation")
    ob.get_user_liked_songs()

    

if __name__ == "__main__":
    run_retrieve_liked_songs()