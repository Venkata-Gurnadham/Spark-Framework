import yaml
from pyspark.sql import SparkSession
def load_config(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def get_spark_session(app_name, master, spark_config):
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    for key, value in spark_config.items():
        spark.conf.set(key, value)
    return spark





# from pyspark.sql import SparkSession
# import yaml

# def get_spark_session(app_name: str, master: str, spark_config: dict):
#     spark = SparkSession.builder.appName(app_name).master(master)
#     for key, value in spark_config.items():
#         spark = spark.config(key, value)
#     return spark.getOrCreate()

# def load_config(file_path: str):
#     with open(file_path, 'r') as f:
#         return yaml.safe_load(f)
