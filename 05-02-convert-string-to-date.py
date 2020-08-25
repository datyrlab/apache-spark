#!/usr/bin/python3

######################################
# PySpark - Date formatting, convert string to Timestamp Date Object - Part 5.2
# https://youtu.be/skJ4kzmvVg0
# 0:00 - quick intro, copy script from previous tutorial part 5.1
# 4:43 - pyspark.sql.functions, spark date formatter, convert string to date object... df.withColumn()
# 19:29 - SQL new date columns added to dataframe... df.sql(...WHERE date BETWEEN '' AND '')
# 26:36 - get the data and use a Python for loop to iterate through dataframe rows... df.rdd.collect()

# script: https://github.com/datyrlab/apache-spark/blob/master/05-02-convert-string-to-date.py

# install dependancies
# sudo pip3 install colorama

# PySpark - SQL MongoDB Twitter JSON data - Part 5.1
# https://youtu.be/lz3V8bYZw9g

# https://twitter.com/datyrlab
######################################

# import os, re
# from colorama import init
# from colorama import Fore, Back, Style
# init(autoreset=True)

import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

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

# convert date string to object
df01 = df.withColumn('created_date_cleaned', f.regexp_replace(f.col("created_at"), "^[A-Za-z]+", "")) \
    .withColumn('created_timestamp', f.to_timestamp(f.col("created_date_cleaned"), " MMM dd HH:mm:ss Z yyyy")) \
    .withColumn('created_date', f.to_date(f.col("created_timestamp"), "MMM dd HH:mm:ss Z yyyy"))

# create a view
df01.createOrReplaceTempView("twitter_user_timeline_guardian") 

# query
SQL_QUERY = "SELECT id, created_at, full_text, created_date FROM twitter_user_timeline_guardian WHERE created_date BETWEEN '2020-05-01' and '2020-05-31'"
df02 = my_spark.sql(SQL_QUERY)

# print(df02.show())
# print(df.printSchema())
# print(df.first())

# iterate rows
for row in df02.rdd.collect():
    print(row.created_at, row.full_text)





