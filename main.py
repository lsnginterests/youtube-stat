from dataplatform.setting.spark_session import get_spark
from dataplatform.setting.dlh_config import DLHConfig

spark = get_spark()

cfg = DLHConfig(spark)
cfg.initiate_infrastructure()

cfg.show_namespaces('local')
spark.sql('drop table if exists local.silver.h_channel')
spark.stop()