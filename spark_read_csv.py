#!/usr/bin/python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
#import SQLContext.implicits._
import csv

APP_NAME = "Spark-ETL"
conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")
sc = SparkContext(conf=conf)
#sqlContext = HiveContext(sc)
sqlContext = SQLContext(sc)

# Create RDD from csv file directly in Spark Context
raw_data = sc.textFile('people.csv')#.map(lambda x: x.split(","))
print("RDD from raw csv file: ", raw_data.collect())
csv_data = raw_data.map(lambda x: x.split(","))
print("RDD from raw RDD file: ", csv_data.collect())

# RDD to DF
df_csv = csv_data.toDF()
df_csv.show()
