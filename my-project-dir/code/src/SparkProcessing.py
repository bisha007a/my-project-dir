from pyspark.sql import *

spark = SparkSession.builder.master("local[2]") \
    .appName("JSON loader").getOrCreate()

a = spark.read.json("/home/hadoop/my-project-dir/my-project-dir/data")
a.printSchema()

b = spark.read.json("/home/hadoop/Desktop/page_turns.json")
b.printSchema()



