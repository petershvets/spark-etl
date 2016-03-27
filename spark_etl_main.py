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
# App name will serve as Client ID
APP_NAME = "Spark-ETL"
conf = SparkConf().setAppName(APP_NAME)
#conf = SparkConf().setAppName(APP_NAME).set("spark.cores.max", "8").setMaster("spark://pdpshvets.modeln.com:7077")
conf = SparkConf().setAppName(APP_NAME).set("spark.cores.max", "8").setMaster("local[4]")
sc = SparkContext(conf=conf)
#HiveContect is a superset of SQLContext, so creating HiveContext
# Can switch to sqlContext = SQLContext(sc)
sqlContext = HiveContext(sc)

# Access app metadata repository and read execution metadata
mysql_driver = 'com.mysql.jdbc.Driver'
mysql_jdbc_prop = "jdbc:mysql://"
mysql_host_prop = "mysql.rds.amazonaws.com/"
meta_db_prop = "SPARK_DB"
meta_table = "(SELECT * from sp_etl_app_data WHERE CLIENT_ID = 'Cust_ID') ta"
#v_properties = {"user":"spark_dm", "password":"spark_dm"} 
db_user = 'spark'
db_user_passwd = 'spark'
v_url = mysql_jdbc_prop + mysql_host_prop + meta_db_prop

#.option("url", "jdbc:mysql://mysqlrevvyanalytics01.cbjfkl3dhwj2.us-west-1.rds.amazonaws.com/SPARK_DB")
df_metadata_repo = sqlContext.read.format("jdbc").option("url", v_url) \
.option("driver", mysql_driver) \
.option("dbtable", meta_table).option("user", db_user).option("password", db_user_passwd).load()



print("Object type from JDBC: ", type(df_metadata_repo))
df_metadata_repo.show()

# Get app environment variables
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
