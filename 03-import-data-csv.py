#!/usr/bin/python3

######################################
# PySpark - Open text file, import data CSV into an RDD - Part 3
# https://youtu.be/pGiWdaQxpuQ
# 0:00 - quick intro, create python file and copy SparkContext connection from previous tutorial
# 2:18 - open Netflix csv data file in vim editor for quick view of it's content and copy file path
# 2:57 - add csv file to python script and import data as RDD. Run code, view RDD object and view it's class attributes
# 8:22 - print total count of rows and first row
# 12:15 - lambda function to split string column delimiter into column indexes (so RDD data has both rows and columns)
# 17:11 - update split in lamda function to a regular expression, to fix delimiter any issues
# 27:02 - for loop through each row to print or do something with each column value

# script: https://github.com/datyrlab/apache-spark/blob/master/03-import-data-csv.py

# PySpark - Getting started, SparkContext & SparkSession Instances - Part 2
# https://youtu.be/Up7q_ayhRM4

# https://twitter.com/datyrlab
######################################

import re
from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("mytest").setMaster("local[*]")
sc = SparkContext(conf=conf)

# change file path to where you have saved the csv file
file_rdd= sc.textFile("/home/piuser/Desktop/tutorial/spark/data/netflix_titles.csv")

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''') 

rows = file_rdd \
    .map(lambda line : COMMA_DELIMITER.split(line))

listofrows = rows.map(lambda row: row).collect()
print(listofrows)

