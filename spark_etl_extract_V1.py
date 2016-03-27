#!/usr/bin/python

from simple_salesforce import Salesforce
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from pyspark import Row
from pyspark.sql.types import *
from pyspark.sql import functions
from pyspark.sql.functions import *
from optparse import OptionParser
from pyspark.sql import DataFrameWriter
import json
import re
import os
from datetime import datetime

# *** SPARK-ETL packages
import util

def main(sc, sqlContext, properties_file, spark_etl_logger):
	""" This is main data extraction functionality
		Data is extracted from SFDC and loaded into Spark SQL temp tables
	"""
	startTime = datetime.now()
	# Enable logging
	spark_etl_logger.info("***** Main process execution started at: "+str(startTime))

	# Get app environment variables
	d_app_variables = util.get_app_variables()
	spark_etl_logger.info("Application environment variables: %s" %(d_app_variables))

	spark_etl_logger.info("Processing Spark ETL properties file: %s" %(properties_file))

	##### Get table properties defined in respective table ETL config file ####
	# Store table properties in local dictionary for servicing the script
	#No need to pass SPARK_ETL_CONF_DIR variable as driver script passes file with absolute path
	dict_tbl_properties = util.get_json_config('', properties_file)	

	##### Defined SOQL statement takes precedence over list of source columns #####
	##### SOQL statement will be proccessed and related metadata will be extracted from it
	if len(dict_tbl_properties["soql_query"]) > 0:
		# Process SOQL query if it is defined in config file
		soqlStmt = dict_tbl_properties["soql_query"]
		spark_etl_logger.info("Defined SOQL statement: "+ soqlStmt)
		# Process list of fields and define schema for creating RDD	
		schemaCol = re.findall('SELECT\s(.+)\sFROM', dict_tbl_properties["soql_query"], flags=re.IGNORECASE)[0]
		spark_etl_logger.info("Columns extracted from SOQL: " + schemaCol)
		# Removing extra whitespaces from string elements while converting
		schemaList = [rec.strip() for rec in schemaCol.split(',')] 
		# Convert column names into StructType for RDD
		fields = [StructField(field_name, StringType(), True) for field_name in schemaList]
		schema = StructType(fields)
		# Define source table name - extract from SOQL Query
		src_tbl_name = re.findall("FROM\s(\S+)", soqlStmt, flags=re.IGNORECASE)[0]
		spark_etl_logger.info("Source table name: " + src_tbl_name)
		# Define target table name
		tgt_table_name = dict_tbl_properties["tgt_table"]
		spark_etl_logger.info("Target table name: " + tgt_table_name)
	else:
		spark_etl_logger.info("SOQL statement is not defined, will process src_table and src_columns properties")
		# Constructing SOQL statement from properties provided, converting list to str
		soqlStmt = "SELECT " + ', '.join(dict_tbl_properties["src_columns"]) \
		+ " FROM " \
		+ dict_tbl_properties["src_table"] \
		+ " " + dict_tbl_properties["where"] \
		+ " " + dict_tbl_properties["limit"]  
		spark_etl_logger.info("Constructed SOQL statement: %s" %(soqlStmt))
		# Process list of fields and define schema for creating RDD
		schemaList = dict_tbl_properties["src_columns"]
		spark_etl_logger.info("Schema from config file: %s" %(schemaList))
		fields = [StructField(field_name, StringType(), True) for field_name in schemaList]
		schema = StructType(fields)
		# Define source table name
		src_tbl_name = dict_tbl_properties["src_table"]
		spark_etl_logger.info("Source table name: " + src_tbl_name) 
		# Define target table name for load into target data storage of your choice
		tgt_table_name = dict_tbl_properties["tgt_table"]
		spark_etl_logger.info("Target table name: ",tgt_table_name)

	################### End process table properties defined in table ETL config file ##################

	# Get Salesforce connection details from connections json file
	spark_etl_logger.info("Processing SFDC connections information file sfdc_connections.json")
	d_sfdc_conn = util.get_json_config(d_app_variables['SPARK_ETL_CONN_DIR'], "sfdc_connections.json")
	spark_etl_logger.info("SFDC Connections: %s" %(list(d_sfdc_conn.keys())))

	# Process SFDC Connection details
	spark_etl_logger.info("SFDC Connection details: %s" %(d_sfdc_conn[dict_tbl_properties["sfdc_connection"]]))

	# Establish connection to Salesforce. Using Simple-Salesforce package
	exec("sf=" + util.get_sfdc_conn(**d_sfdc_conn[dict_tbl_properties["sfdc_connection"]]), globals())

	###### Retrieve source table properties - use it to define target table DDL ####
	#
	# Store object description in list of dictionaries
	# This structure returned by Simple-Salesforce
	exec("tblDesc = sf."+src_tbl_name+".describe()", globals())

	lColProperties = ['name', 'type', 'length', 'precision', 'custom', 'scale']
	columnProperties = list()

	for line in tblDesc['fields']: # Iterate through the list of dictionaries
		# Keep only needed properties listed in lColProperties list and 
		# columns mapped in config properties file and remove the rest
		rec = {k:line[k] for k in (lColProperties) if line["name"] in list(dict_tbl_properties["columns_map"].keys())}
		if len(rec) == 0:continue
		columnProperties.append(rec)
		spark_etl_logger.info("Column properties: %s" %(rec))

	# Record table properties in json file
	with open(os.path.join(d_app_variables['SPARK_ETL_LOG_DIR'],tgt_table_name+"_schema.json"), "w") as tableMetadata_file:
		json.dump(columnProperties, tableMetadata_file)

	# Build DDL in order to create table in MySQL db
	for record in columnProperties:
		spark_etl_logger.info("Column MySQL datatype: " + record["name"]+" Type:"+record["type"]+" New: "+util.get_sfdc_mysql_dt(record["type"], str(record["length"]), str(record["precision"]), str(record["scale"])))

	############################ Start Data Acquisition ###########################
	#
	# Extract data from SFDC - run SOQL statement. sf.query returns a list of OrderedDict
	queryResultRaw = sf.query_all(soqlStmt)

	#********************* Clean up dataset *************************#
	# Remove unrelated record metadata provided by SFDC
	queryResult = list()
	for line in queryResultRaw['records']:
		rec=[(k,str(v)) for k, v in line.items() if k not in "attributes"]
		queryResult.append(rec)

	# Create RDD
	v_rdd = sc.parallelize(queryResult)
	rddElemCount = v_rdd.count()
	spark_etl_logger.info("Dataset contains:  "+ str(rddElemCount) + " records")

	# Create DataFrame
	global sqlDataFrame
	sqlDataFrame = v_rdd.map(lambda l: Row(**dict(l))).toDF()
	spark_etl_logger.info("Created data frame with extracted data:: ")
	sqlDataFrame.printSchema()
	sqlDataFrame.show()

	####################### UDF functions #########################
	# Create UDFs
	#
	# logic to handle null values
	slen = udf(lambda s: 0 if s is None else len(s), IntegerType())
	StrConcat = udf(lambda s: "ADD_SOMETHING"+s, StringType())

	####################### End UDF functions #########################

	######################## Mapping columns ############################
	# Create a dict out of column list in form
	for k,v in sorted(dict_tbl_properties["columns_map"].items()):
		#print ("Column mapping: ", k,v)
		spark_etl_logger.info("Column mapping: "+k+":"+v)

	# Construct command for column renaming
	dfColumnsOrig = "sqlDataFrame"
	dfColumnsRenamed = "dfRemapped"
	wCol ='' 
	v_dfSQL_col = ''
	for k,v in sorted(dict_tbl_properties["columns_map"].items()):
		#wCol = wCol + ".withColumn(\'"+v+"\' , "+dfColumnsOrig+"."+k+")"
		wCol = wCol + ".withColumnRenamed(\'"+k+"\' , \'"+v+"\')"
		v_dfSQL_col = v_dfSQL_col + "\""+v+"\","

	dfSQL_col = v_dfSQL_col.rstrip(',')
	spark_etl_logger.info("The following command will be executed: dfRemapped = sqlDataFrame %s" %(wCol))
#	exec(dfColumnsRenamed+" = "+dfColumnsOrig+wCol, globals())
	exec("global dfRemapped; dfRemapped = sqlDataFrame"+wCol, globals())
	dfRemapped.printSchema() 
	dfRemapped.show() 
	######################## End mapping columns ########################

	#################### Register DataFrame as Temp Table for SQL operatoins ####################
	spark_etl_logger.info("Registering remapped data frame as Spark SQL temp table")
	dfRemapped.registerTempTable(tgt_table_name)
	# Run SQL (returns RDD)
#	product = sqlContext.sql("SELECT * FROM "+ tgt_table_name)

	# Persist processed DataFrame into Hive
	sqlContext.sql("USE peter_hive_db")
	sqlContext.sql("DROP TABLE IF EXISTS peter_hive_db.H_"+tgt_table_name)
	spark_etl_logger.info("Persist remapped data frame into Hive")
	dfRemapped.write.saveAsTable("H_"+tgt_table_name)
	
	#Persist data into Amazon AWS S3
	print("Serialize DF into S3")
#	dfRemapped.write(os.path.join("/user/hive/warehouse/",tgt_table_name), "json")
#	dfRemapped.write("s3n://hive-qs-data/"+tgt_table_name, "json", )
	dfRemapped.repartition(1).save("s3n://hive-qs-data/"+tgt_table_name+".json", "json", )	
	print("Done serialize DF into S3")	
	spark_etl_logger.info("Total execution time was: " + str(datetime.now() - startTime))
