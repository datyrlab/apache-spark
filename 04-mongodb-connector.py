#!/usr/bin/python3

######################################
# PySpark - MongoDB Spark Connector, Install MongoDB, use Spark to read NoSQL data- Part 4
# https://youtu.be/-hb-oV_yz54
# 0:00 - intro
# 1:03 - create empty python file ready to write code
# 2:56 - install MongoDb
# 7:02 - start MongoDb server and configure to start on boot
# 9:14 - access Mongo shell to verify Twitter data imported into Mongo database and count documents in collection
# 12:43 - Python script with PySpark MongoDB Spark connector to import Mongo data as RDD, dataframe
# 22:54 - fix issue so MongoDB Spark connector is compatible with Scala version number
# 24:43 - succesful result showing Mongo collection, it's schema for Twitter User Timeline data
# 25:26 - print first row of Twitter data

# PySpark - Open text file, import data CSV into an RDD - Part 3
# https://youtu.be/pGiWdaQxpuQ

# https://twitter.com/datyrlab
######################################

import datetime
from pyspark.sql import SparkSession

# load mongo data
input_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"
output_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"

my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()

df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()

print(df.show())
# print(df.printSchema())
# print(df.first())

