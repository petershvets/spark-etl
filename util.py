#!/usr/bin/python
#

# Set up SPARK-ETL application enviornment variables 
import os
import re
def get_app_variables():
	""" Function sets up application specific
		environment variables
		Args: None
		Returns: dictionary containing Spark-ETL environment variables:
			SPARK_ETL_HOME - application home, top level directory
			SPARK_ETL_CONF_DIR - application configuration directory
			SPARK_ETL_CONN_DIR - application connections directory
			SPARK_ETL_LOG_DIR - application log directory
	"""
	os.chdir(os.path.dirname(__file__))
	CURRENT_DIR=os.getcwd()
#	print("Script is running from: ", CURRENT_DIR)
	vSPARK_ETL_HOME = re.findall('(\S+?spark-etl)', CURRENT_DIR, flags=re.IGNORECASE)[0]
#	print("vSPARK_ETL_HOME = ",vSPARK_ETL_HOME)
	if os.environ.get('SPARK_ETL_HOME') is None:
#		os.environ['SPARK_ETL_HOME'] = os.path.expanduser('~/Spark_SQL/spark-etl')
		os.environ['SPARK_ETL_HOME'] = vSPARK_ETL_HOME
	else:
		os.environ.get('SPARK_ETL_HOME')

	d_env_variables=dict()
	d_env_variables['SPARK_ETL_HOME'] = os.environ.get('SPARK_ETL_HOME')
	d_env_variables['SPARK_ETL_CONF_DIR'] = os.environ.get('SPARK_ETL_HOME')+"/etl-config/"
	d_env_variables['SPARK_ETL_CONN_DIR'] = os.environ.get('SPARK_ETL_HOME')+"/connections/"
	d_env_variables['SPARK_ETL_LOG_DIR'] = os.environ.get('SPARK_ETL_HOME')+"/logs/"

	return d_env_variables

# Setup logging
import logging
import logging.handlers
def set_up_logging():

    # File handler for log file
    logDir = get_app_variables()['SPARK_ETL_LOG_DIR']
    logFile = 'spark_etl.log'
    serverlog = logging.FileHandler(logDir+logFile)    
    serverlog.setLevel(logging.DEBUG)
    serverlog.setFormatter(logging.Formatter('%(asctime)s - %(name)s : %(levelname)s %(message)s'))

    # Logger 
    logger = logging.getLogger('SPARK-ETL')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(serverlog)

    return logger
##### End of logging setup

# Reading content of json files.
import json
# Process Spark ETL json property file
def get_json_config(file_path, in_json_file):
	""" Args:
			file_path
			in_json_file
		Returns:
			dictionary of properties defined in passed properties file
			returned dict and its content is context dependent
	   Raises:
		json.decoder.JSONDecodeError: if config file is not valid JSON document			
	"""
		
	json_file_content_str = open(file_path + in_json_file, encoding='utf-8').read()	
	try:
		out_json_properties = json.loads(json_file_content_str, encoding='utf-8')
		return out_json_properties
	except json.decoder.JSONDecodeError:
		print("Provided config file "+in_json_file+" is not valid JSON document")
#		logger.error("Provided config file: %s " %(in_json_file))
		exit(1)

# Function builds SFDC connection string
def get_sfdc_conn(**p_dict):
	""" Args: dictionary with SFDC connection credentials
		Returns: SFDC connection string required by Simple-salesforce API
		Syntax SFDC connection:
			sf = Salesforce(username='', password='', security_token='')
	"""
	sfdc_conn = "Salesforce(username='" + p_dict['username'] + "', password='" + p_dict['password'] + "', security_token='" + p_dict['security_token']+"')"
	return sfdc_conn

# Create datatype mapping  SFDC-MySQL
def get_sfdc_mysql_dt(sfdc_dtype, sfdc_length, sfdc_precision, sfdc_scale):
	"""Function converts SFDC datatypes into MySQL compatible datatypes
		Args:
			sfdc_datatype, sfdc_length, sfdc_precision, fdc_scale
		Returns:
			dictionary of MySQL compatible datatypes
	"""
	d_sfdc_mysql_dtype_map = {
	"id":"varchar",
	"boolean":"decimal(1,0)",
	"string":"varchar",
	"picklist":"varchar",
	"datetime":"timestamp(6)",
	"date":"timestamp(6)",
	"reference":"varchar",
	"double":"decimal",
	"textarea":"varchar"
	}

	if sfdc_dtype == "boolean": return d_sfdc_mysql_dtype_map[sfdc_dtype]
	elif sfdc_dtype == "datetime": return d_sfdc_mysql_dtype_map[sfdc_dtype]
	elif sfdc_dtype == "date": return d_sfdc_mysql_dtype_map[sfdc_dtype]
	elif sfdc_dtype == "double": return d_sfdc_mysql_dtype_map[sfdc_dtype]+"("+sfdc_precision+", "+sfdc_scale+")"
	elif sfdc_dtype == "string": return d_sfdc_mysql_dtype_map[sfdc_dtype]+"("+sfdc_length+")"
	elif sfdc_dtype == "id": return d_sfdc_mysql_dtype_map[sfdc_dtype]+"("+sfdc_length+")"
	else:
		return 'None'
#
