#!/usr/bin/python
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import os
from multiprocessing import Pool
# Import Spark-ETL packages
import spark_etl_extract_V1
from util import get_app_variables, set_up_logging

# Start logging
spark_etl_logger = set_up_logging()

# Initiate Spark app, Spark Context and HiveContext
APP_NAME = "Spark-ETL"
conf = SparkConf().setAppName(APP_NAME)
conf = SparkConf().setAppName(APP_NAME).setMaster("spark://<host name>:7077")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

###############################################################
# Test reading from S3
print("Prepping hadoop for reading S3 file")
#hadoopConf = SparkContext.hadoopConfiguration
#sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIJCHQUJC4PMAILDA")
#sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "/home/pshvets/Spark_SQL/ricaws.pem") # can contain "/"
myRDD = sc.textFile("s3n://hive-qs-data/employee.csv")
s3file = myRDD.count()
print("File count on S3: ", str(s3file))
for rec in myRDD.collect():
	print("Reading S3 file: ", rec)
print("Done with reading file from S3")
###############################################################

# Create customer specific Hive database.
# This 
sqlContext.sql("CREATE DATABASE IF NOT EXISTS peter_hive_db")

# Create pool for parallel processing
pool = Pool()

# Get environment variables
app_variables = get_app_variables()
# Compile a list of all property files under $SPARK_ETL_CONF_DIR folder
path = app_variables.get('SPARK_ETL_CONF_DIR')
prop_files = [os.path.join(path,fileiter) for fileiter in os.listdir(path) if fileiter.endswith('.json')]
print (prop_files)

# Data Extract
if __name__ == "__main__":
	# Execute core functionality. Iterate over all propertry files in Spark-ETL config directory
	for prop_fileiter in prop_files:
		spark_etl_extract_V1.main(sc, sqlContext, prop_fileiter, spark_etl_logger)


		
# Data Transformation
#  TBD
# Data Load
#  TBD

# Test
#product = sqlContext.sql("SELECT * FROM MN_PRODUCT_DIM")
#for record in product.collect():
#	print ("Main - iterating over SQL query result: ",record)

prod_family=sqlContext.sql("select p.PRODUCT_NAME, pf.PRODUCT_FAMILY_NAME from MN_PRODUCT_DIM p JOIN MN_PRODUCT_FAMILY_DIM pf ON p.PRODUCT_FAMILY_ID = pf.PRODUCT_FAMILY_ID where p.PRODUCT_NAME = 'Adbil_1_10 mL'" )

#prod_family.take(1)
#for rec in prod_family.collect():
#	print("Joined tables:", rec)

#Hive table
sqlContext.sql("CREATE TABLE IF NOT EXISTS H_EMPLOYEE(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
sqlContext.sql("TRUNCATE TABLE H_EMPLOYEE")
sqlContext.sql("LOAD DATA LOCAL INPATH '/home/pshvets/Spark_SQL/spark-etl/datafiles/employee.txt' INTO TABLE H_EMPLOYEE")
emp_result = sqlContext.sql("FROM H_EMPLOYEE SELECT name")

for rec in emp_result.collect():
	print ("Hive table H_EMPLOYEE.name: ", rec)

emp = sqlContext.sql("SELECT * FROM H_EMPLOYEE")
for rec in emp.collect():
	print("Spark SQL from Hive: ", rec)

