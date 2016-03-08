#!/usr/bin/python
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import os
from multiprocessing import Pool
# Import Spark-ETL packages
import spark_etl_extract
from util import get_app_variables, set_up_logging

# Start logging
spark_etl_logger = set_up_logging()

# Initiate Spark app, Spark Context and HiveContext
APP_NAME = "Spark-ETL"
conf = SparkConf().setAppName(APP_NAME)
conf = SparkConf().setAppName(APP_NAME).set("spark.cores.max", "4").setMaster("local")
sc = SparkContext(conf=conf)
#HiveContect is a superset of SQLContext, so creating HiveContext
# Can switch to sqlContext = SQLContext(sc)
sqlContext = HiveContext(sc)

# Get environment variables
app_variables = get_app_variables()
# Compile a list of all property files under $SPARK_ETL_CONF_DIR folder
path = app_variables.get('SPARK_ETL_CONF_DIR')
prop_files = [os.path.join(path,fileiter) for fileiter in os.listdir(path) if fileiter.endswith('.json')]
spark_etl_logger.info("The following tables will be processed %s" %(prop_files))


# Data Extract
if __name__ == "__main__":
	# Execute core functionality. Iterate over all propertry files in Spark-ETL config directory
	for prop_fileiter in prop_files:
		spark_etl_extract.main(sc, sqlContext, prop_fileiter, spark_etl_logger)

# Data Transformation
#  TBD
# Data Load
#  TBD
