#!/usr/bin/python3

######################################
# PySpark - Joins on multiple DataFrames from different data types - Part 6.2
# https://youtu.be/Q1DOEn9_Obw
# 0:00 - intro
# 0:45 - copy script from previous tutorial
# 2:58 - DataFrame: Left join Twitter data followers & friends
# 15:15 - SQL: from dataframe create SQL tables with createOrReplaceTempView()
# 24:14 - SQL: query Left join Twitter data followers & friends
# 36:07 - quick analysis & insights from join result of Twitter data
# 37:39 - why Spark makes it easier

# script DF JSON: https://github.com/datyrlab/apache-spark/blob/master/06-02-01-join-df-json.py
# script DF MongoDB: https://github.com/datyrlab/apache-spark/blob/master/06-02-01-join-df-mongodb.py  
# script SQL JSON: https://github.com/datyrlab/apache-spark/blob/master/06-02-02-join-sql-json.py  
# script SQL MongoDB: https://github.com/datyrlab/apache-spark/blob/master/06-02-02-join-sql-mongodb.py

# install dependancies
# sudo pip3 install colorama

# Download Twitter files
# https://github.com/datyrlab/apache-spark/blob/master/data/twitter_followers/the_moyc.json
# https://github.com/datyrlab/apache-spark/blob/master/data/twitter_friends/the_moyc.json

# PySpark - Import multiple DataFrames from JSON, CSV and MongoDB - Part 6.1
# https://youtu.be/4cHTJihTHS4

# https://twitter.com/datyrlab
######################################

import os, re
from colorama import init, Fore, Back, Style
init(autoreset=True)

import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

COLLECTION = "the_moyc"

# spark session
my_spark = SparkSession\
    .builder\
    .appName("twitter")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .master("local[*]")\
    .getOrCreate()

# load mongo data
df_followers = my_spark.read.format('com.mongodb.spark.sql.DefaultSource')\
    .option("uri", f"mongodb://127.0.0.1/twitter_followers.{COLLECTION}")\
    .load()

df_friends = my_spark.read.format('com.mongodb.spark.sql.DefaultSource')\
    .option("uri", f"mongodb://127.0.0.1/twitter_friends.{COLLECTION}")\
    .load()

# create temp tables for SQL
print(Fore.WHITE + Back.MAGENTA + f"create sql temp table views")
df_followers.createOrReplaceTempView("twitter_followers")
df_friends.createOrReplaceTempView("twitter_friends")

print(my_spark.catalog.listTables())

# SQL query
print(Fore.WHITE + Back.MAGENTA + f"join")
query = """
SELECT
    t1.id_str           AS id_str ,
    t1.created_at       AS created_at,
    t1.created_at_date  AS created_at_date,
    t1.screen_name      AS screen_name,
    t1.followers_count  AS followers_count,
    t1.friends_count    AS friends_count,
    t1.screen_name      AS fr_screen_name

FROM twitter_followers AS t1
LEFT JOIN twitter_friends AS t2
ON t1.screen_name = t2.screen_name
ORDER BY t1.followers_count DESC
"""

df = my_spark.sql(query)
#print( df.show( df.count(), truncate=False ) )
print( df.show( 100, truncate=False ) )
print( df.count() )

# stop session
my_spark.stop()








