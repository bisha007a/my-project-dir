from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkSessionCreate:
    @staticmethod
    def get_spark_session(app_name):
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark
