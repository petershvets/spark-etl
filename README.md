# spark-etl
Spark-ETL is Data Integration tool built on Spark and Simple-Salesforce.

	Spark - http://spark.apache.org/downloads.html. Version 1.5.2 was used for current implementation.
	Simple-Salesforce - https://github.com/heroku/simple-salesforce

#Dependencies:
	Python3
	Simple-Salesforce
	Spark 1.5.2

#Functionality
SPARK-ETL application performs the following operations:

	Connects to Salesforce org
	Extracts data from tables defined in property files
	Loads each respective table data into Spark DataFrame
	Creates Spark SQL table for further operations

#Structure:
There is top level folder called 'spark-etl' and four subfolders: 'connections', 'etl-config', 'logs' and 'scripts'
spark-etl

	connections - contains connection information in json files
	etl-config - contains table 
	logs
	scripts
  
#Installation/Configuration
###Environment and Application Configuration

Environment Variables


# Troubleshooting

# Future functionality
	Incremental extraction
	Load data into relational database (PostgreSQL, MySQL)
