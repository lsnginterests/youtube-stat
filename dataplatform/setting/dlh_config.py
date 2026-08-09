from pyspark.sql import SparkSession

from config import get_settings

class DLHConfig:

    def __init__(self, session: SparkSession):
        self._session = session
        self.RAW_PATH = f's3a://{get_settings().s3_bucket_raw}/youtube'

    def initiate_infrastructure(self):
        self._session.sql('create namespace if not exists local.bronze')
        self._session.sql('create namespace if not exists local.silver')
        self._session.sql('create namespace if not exists local.gold')

    def show_namespaces(self, location: str):
        return self._session.sql(f'show namespaces in {location}').show()