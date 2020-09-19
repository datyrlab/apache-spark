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
    .master("local[*]")\
    .getOrCreate()

# get data from JSON files
file = f'/home/piuser/Desktop/tutorial/spark/data/twitter_followers/{COLLECTION}.json'
df_followers = my_spark.read.json(file)

file = f'/home/piuser/Desktop/tutorial/spark/data/twitter_friends/{COLLECTION}.json'
df_friends = my_spark.read.json(file)

# select columns (key names) from dataframe
dffollowers = df_followers.select("id_str", "created_at", "created_at_date", "screen_name", "followers_count", "friends_count")
dffriends = df_friends.select("id_str", "created_at", "created_at_date", "screen_name", "followers_count", "friends_count")

print(Fore.WHITE + Back.MAGENTA + f"dffollowers {type(dffollowers)}")
print(dffollowers.show())

print(Fore.WHITE + Back.MAGENTA + f"dffriends {type(dffriends)}")
print(dffriends.show())

# join dataframes
print(Fore.WHITE + Back.MAGENTA + f"join")
df = dffollowers.join( dffriends, dffollowers.screen_name == dffriends.screen_name, how='left' )
print(df.show(df.count()))
print(df.count())

# stop session
my_spark.stop()
