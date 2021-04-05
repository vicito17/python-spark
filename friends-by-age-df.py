from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("friends-by-age-df").getOrCreate()

df = spark.read.option("header",True).csv("data/fakefriends-header.csv")

df.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show(80)