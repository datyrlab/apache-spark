#!/usr/bin/python3

######################################
# PySpark - Getting started, SparkContext & SparkSession Instances - Part 2
# https://youtu.be/Up7q_ayhRM4
# 0:00 - intro
# 0:55 - create two empty python files for SparkContext and SparkSession
# 2:52 - find number of threads available on my machine (cpuinfo)
# 4:32 - create an instance for SparkContext
# 13:22 - create an instance for SparkSession

# Apache Spark - Install Spark3, PySpark3 on Ubuntu 20.04, Debian, Python 3.8 - Part 1b
# https://youtu.be/snZvQcI2HfQ

# Install Apache Spark and PySpark on Ubuntu 20.04 Linux Debian, Python 3.7 - Part 1a
# https://youtu.be/_qd--H1jBbw

# https://twitter.com/datyrlab
######################################

from pyspark.sql import SparkSession


sc = SparkSession.builder.appName('mytest').getOrCreate() 

print(type(sc),"\n")
print(dir(sc),"\n") 
print(sc.version,"\n")
