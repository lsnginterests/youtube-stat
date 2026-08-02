from pyspark.sql import SparkSession

class DLHConfig:

    RAW_PATH = '/home/maxim/user/programming/youtube-stat/dataplatform/raw/youtube'
    BRONZE_PATH = '/home/maxim/user/programming/youtube-stat/dataplatform/lakehouse/bronze'
    SILVER_PATH = '/home/maxim/user/programming/youtube-stat/dataplatform/lakehouse/silver'
    GOLD_PATH = '/home/maxim/user/programming/youtube-stat/dataplatform/lakehouse/gold'

    def __init__(self, session: SparkSession):
        self._session = session

    def initiate_infrastructure(self):
        self._session.sql('create namespace if not exists local.bronze')
        self._session.sql('create namespace if not exists local.silver')
        self._session.sql('create namespace if not exists local.gold')

    def show_namespaces(self, location: str):
        return self._session.sql(f'show namespaces in {location}').show()