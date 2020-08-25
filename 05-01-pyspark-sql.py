#!/usr/bin/python3

######################################
# PySpark - SQL MongoDB Twitter JSON data - Part 5.1
# https://youtu.be/lz3V8bYZw9g
# 0:00 - intro 
# 0:23 - copy MongoDB connector script from previous tutorial part 4
# 1:30 - run script MongoDB connector, show first row... df.first()
# 3:04 - modify script to show more rows... df.show()
# 4:23 - import colorama, datetime, add date variables, format ISO date
# 15:24 - add date and regex query to pipeline MongoDB connector... options('pipeline', pipeline)
# 35:11 - create a temp view and SQL query... df.createOrReplaceTempView(), df.sql()
# 41:27 - expand columns by disabling truncation... df.show(truncate=False) 
# 42:49 - show all rows... df.show(df.count())

# script: https://github.com/datyrlab/apache-spark/blob/master/05-01-pyspark-sql.py

# install dependancies
# sudo pip3 install colorama

# PySpark - MongoDB Spark Connector, Install MongoDB, use Spark to read NoSQL data- Part 4
# https://youtu.be/-hb-oV_yz54

# https://twitter.com/datyrlab
######################################

import os, re
from colorama import init
from colorama import Fore, Back, Style
init(autoreset=True)

import datetime
from pyspark.sql import SparkSession

# load mongo data
input_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"
output_uri = "mongodb://127.0.0.1/twitter_user_timeline.guardian"

my_spark = SparkSession\
    .builder\
    .appName("twitter")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()

# date range
date_start = str(datetime.datetime.strptime("2020-05-01", '%Y-%m-%d').isoformat()) + "Z" 
date_end = str(datetime.datetime.strptime("2020-05-30", '%Y-%m-%d').isoformat()) + "Z"
print(Fore.GREEN + Back.WHITE + f"date_start: {date_start}", Fore.GREEN + Back.WHITE + f"date_end: {date_end}")


pipeline = { \
    '$match': { \
        '$and': [ \
             { \
                'created_at_date': { \
                    '$gt': { \
                        '$date': date_start \
                    } \
                } \
             }, \
             { \
                'created_at_date': { \
                    '$lt': { \
                        '$date': date_end \
                    } \
                } \
            }  \
        ], \
        'full_text': { \
            '$regex':'covid', \
            '$options':'i' \
        } \
    } \
} 

df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').option("pipeline", pipeline).load()

# create view
df.createOrReplaceTempView("twitter_user_timeline_guardian")

# query
SQL_QUERY = "SELECT id, created_at, full_text, text FROM twitter_user_timeline_guardian"
new_df = my_spark.sql(SQL_QUERY)

print(new_df.show(df.count(), truncate=False))
# print(df.printSchema())
# print(df.first())




