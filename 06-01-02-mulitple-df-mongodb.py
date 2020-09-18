#!/usr/bin/python3

######################################
# PySpark - Import multiple data from JSON, CSV and MongoDB - Part 6.1
# https://youtu.be/4cHTJihTHS4

# 0:00 - intro
# 2:57 - copy script from previous tutorial
# 8:12 - import data json: reference path to Twitter files
# 13:17 - import data json: validate import of multiple dataframes 
# 17:52 - import data json: select columns from dataframe
# 24:35 - import data json: validate select on columns
# 26:57 - import data csv: reference path to Netflix file & csv options
# 34:52 - import data csv: select columns from dataframe
# 36:34 - import data csv: validate import of multiple dataframes, csv & json
# 40:19 - import data MongoDB: copy script from previous MongoDB tutorial 
# 42:42 - import data MongoDB: import multiple databases & collections
# 57:46 - import data MongoDB: validate import of multiple dataframes

# script: https://github.com/datyrlab/apache-spark/blob/master/06-01-01-mulitple-df-json-csv.py
# script: https://github.com/datyrlab/apache-spark/blob/master/06-01-02-mulitple-df-mongodb.py 

# install dependancies
# sudo pip3 install colorama

# PySpark - Date formatting, convert string to Timestamp Date Object - Part 5.2
# https://youtu.be/skJ4kzmvVg0

# https://twitter.com/datyrlab
######################################

import os, re
from colorama import init, Fore, Back, Style
init(autoreset=True)

import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

COLLECTION = "the_moyc"

# load mongo data
my_spark = SparkSession\
    .builder\
    .appName("twitter")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .master("local[*]")\
    .getOrCreate()

df_followers = my_spark.read.format('com.mongodb.spark.sql.DefaultSource')\
    .option("uri", f"mongodb://127.0.0.1/twitter_followers.{COLLECTION}")\
    .load()

df_friends = my_spark.read.format('com.mongodb.spark.sql.DefaultSource')\
    .option("uri", f"mongodb://127.0.0.1/twitter_friends.{COLLECTION}")\
    .load()

# print
#print(Fore.WHITE + Back.RED + f"followers count: {df_followers.count()}", Fore.WHITE + Back.GREEN + f"followers: {df_followers.first()}")
#print(Fore.WHITE + Back.RED + f"friends count: {df_friends.count()}", Fore.WHITE + Back.CYAN + f"friends: {df_friends.first()}")

dffollowers = df_followers.select("id_str", "created_at", "created_at_date", "screen_name", "followers_count", "friends_count")
dffriends = df_friends.select("id_str", "created_at", "created_at_date", "screen_name", "followers_count", "friends_count")

print(Fore.WHITE + Back.MAGENTA + f"dffollowers {type(dffollowers)}")
print(dffollowers.show())

print(Fore.WHITE + Back.MAGENTA + f"dffriends {type(dffriends)}")
print(dffriends.show())

# stop session
my_spark.stop()
